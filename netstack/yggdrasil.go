package netstack

import (
	"net"
	"sync"

	"github.com/yggdrasil-network/yggdrasil-go/src/core"
	"github.com/yggdrasil-network/yggdrasil-go/src/ipv6rwc"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

type YggdrasilNIC struct {
	stack      *YggdrasilNetstack
	ipv6rwc    *ipv6rwc.ReadWriteCloser
	dispatcher stack.NetworkDispatcher
	bufPool    *sync.Pool
	rstPackets chan *stack.PacketBuffer
	closeChan  chan struct{}
	closeOnce  sync.Once
}

// debugLogFn is how the netstack layer reaches the application logger. Raw
// log.Println leaked "RWC read error: ErrClosed" onto stderr for every peer
// teardown, once per NIC, in a format the rest of the tool does not use.
var (
	debugLogMu sync.Mutex
	debugLogFn func(string, ...any)
)

// SetDebugLogger installs the logger used for netstack-internal diagnostics.
// It must be called before any NIC is created.
func SetDebugLogger(fn func(string, ...any)) {
	debugLogMu.Lock()
	defer debugLogMu.Unlock()
	debugLogFn = fn
}

func debugLogf(format string, args ...any) {
	debugLogMu.Lock()
	fn := debugLogFn
	debugLogMu.Unlock()
	if fn != nil {
		fn(format, args...)
	}
}

func (s *YggdrasilNetstack) NewYggdrasilNIC(ygg *core.Core) tcpip.Error {
	rwc := ipv6rwc.NewReadWriteCloser(ygg)
	mtu := rwc.MTU()
	nic := &YggdrasilNIC{
		stack:   s,
		ipv6rwc: rwc,
		bufPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, mtu)
			},
		},
		rstPackets: make(chan *stack.PacketBuffer, 100),
		closeChan:  make(chan struct{}),
	}
	if err := s.stack.CreateNIC(1, nic); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-nic.closeChan:
				return
			default:
			}

			readBuf := nic.bufPool.Get().([]byte)
			rx, err := nic.ipv6rwc.Read(readBuf)
			if err != nil {
				nic.bufPool.Put(readBuf)
				select {
				case <-nic.closeChan:
					return
				default:
				}
				// ErrClosed here is normal teardown order: core.Close() ends the
				// read before the NIC's closeChan is signalled.
				debugLogf("Yggdrasil RWC read error: %v", err)
				return
			}

			// Copy payload data into a dedicated slice for stack buffer delivery
			// to guarantee no concurrent modification race condition occurs.
			payloadData := make([]byte, rx)
			copy(payloadData, readBuf[:rx])
			nic.bufPool.Put(readBuf)

			pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
				Payload: buffer.MakeWithData(payloadData),
			})
			if nic.dispatcher != nil {
				nic.dispatcher.DeliverNetworkPacket(ipv6.ProtocolNumber, pkb)
			}
			pkb.DecRef()
		}
	}()

	go func() {
		for {
			select {
			case <-nic.closeChan:
				return
			case pkt, ok := <-nic.rstPackets:
				if !ok || pkt == nil {
					return
				}
				_ = nic.writePacket(pkt)
				pkt.DecRef()
			}
		}
	}()

	_, snet, err := net.ParseCIDR("0200::/7")
	if err != nil {
		return &tcpip.ErrBadAddress{}
	}
	subnet, err := tcpip.NewSubnet(
		tcpip.AddrFromSlice(snet.IP.To16()),
		tcpip.MaskFrom(string(snet.Mask)),
	)
	if err != nil {
		return &tcpip.ErrBadAddress{}
	}
	s.stack.AddRoute(tcpip.Route{
		Destination: subnet,
		NIC:         1,
	})
	if s.stack.HandleLocal() {
		ip := ygg.Address()
		if err := s.stack.AddProtocolAddress(
			1,
			tcpip.ProtocolAddress{
				Protocol:          ipv6.ProtocolNumber,
				AddressWithPrefix: tcpip.AddrFromSlice(ip.To16()).WithPrefix(),
			},
			stack.AddressProperties{},
		); err != nil {
			return err
		}
	}
	return nil
}

func (e *YggdrasilNIC) Attach(dispatcher stack.NetworkDispatcher) { e.dispatcher = dispatcher }

func (e *YggdrasilNIC) IsAttached() bool { return e.dispatcher != nil }

func (e *YggdrasilNIC) MTU() uint32 { return uint32(e.ipv6rwc.MTU()) }

func (e *YggdrasilNIC) SetMTU(uint32) {}

func (*YggdrasilNIC) Capabilities() stack.LinkEndpointCapabilities { return stack.CapabilityNone }

func (*YggdrasilNIC) MaxHeaderLength() uint16 { return 40 }

func (*YggdrasilNIC) LinkAddress() tcpip.LinkAddress { return "" }

func (*YggdrasilNIC) SetLinkAddress(tcpip.LinkAddress) {}

func (*YggdrasilNIC) Wait() {}

func (e *YggdrasilNIC) writePacket(
	pkt *stack.PacketBuffer,
) tcpip.Error {
	defer func() {
		_ = recover()
	}()

	vv := pkt.ToView()
	writeBuf := e.bufPool.Get().([]byte)
	defer e.bufPool.Put(writeBuf)

	n, err := vv.Read(writeBuf)
	if err != nil {
		return &tcpip.ErrAborted{}
	}
	_, err = e.ipv6rwc.Write(writeBuf[:n])
	if err != nil {
		return &tcpip.ErrAborted{}
	}
	return nil
}

func (e *YggdrasilNIC) WritePackets(
	list stack.PacketBufferList,
) (int, tcpip.Error) {
	// written counts the packets that actually made it out; the caller uses
	// the count to account for its work, so a stale or negative number here
	// would misreport every batch.
	written := 0
	for _, pkt := range list.AsSlice() {
		if pkt.Data().Size() == 0 {
			if pkt.Network().TransportProtocol() == tcp.ProtocolNumber {
				tcpHeader := header.TCP(pkt.TransportHeader().Slice())
				if (tcpHeader.Flags() & header.TCPFlagRst) == header.TCPFlagRst {
					pkt.IncRef()
					select {
					case e.rstPackets <- pkt:
						// Packet queued successfully
					default:
						// Channel full, drop packet and release ref
						pkt.DecRef()
					}
					continue
				}
			}
		}
		if err := e.writePacket(pkt); err != nil {
			debugLogf("Yggdrasil writePackets failed: %v", err)
			return written, err
		}
		written++
	}

	return written, nil
}

func (e *YggdrasilNIC) WriteRawPacket(*stack.PacketBuffer) tcpip.Error {
	panic("not implemented")
}

func (*YggdrasilNIC) ARPHardwareType() header.ARPHardwareType {
	return header.ARPHardwareNone
}

func (e *YggdrasilNIC) AddHeader(*stack.PacketBuffer) {
}

func (e *YggdrasilNIC) ParseHeader(*stack.PacketBuffer) bool {
	return true
}

func (e *YggdrasilNIC) Close() {
	e.closeOnce.Do(func() {
		close(e.closeChan)
		if e.stack != nil && e.stack.stack != nil {
			e.stack.stack.RemoveNIC(1)
		}
		e.dispatcher = nil
	})
}

func (e *YggdrasilNIC) SetOnCloseAction(func()) {}
