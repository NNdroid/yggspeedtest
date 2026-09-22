package engine

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"yggspeedtest/netstack"
)

// runDownloadTestMultiStream pulls the test URL through the Yggdrasil netstack
// on `streams` parallel connections and reports the average and peak rates.
//
// Every byte is attributed to the interval it arrived in, which is what makes
// the peak figure meaningful: a single slow chunk at the start no longer drags
// the average down while hiding that the link could do better.
func runDownloadTestMultiStream(
	ctx context.Context,
	ns *netstack.YggdrasilNetstack,
	testURL string,
	customHost string,
	customSNI string,
	streams int,
	maxDuration time.Duration,
	maxBytes int64,
) (avgMbps float64, peakMbps float64, totalBytes int64, durationSec float64, err error) {
	if streams < 1 {
		streams = 1
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	if customSNI != "" {
		tlsConfig.ServerName = customSNI
	}

	transport := &http.Transport{
		DialContext:         ns.DialContext,
		DisableKeepAlives:   true,
		TLSClientConfig:     tlsConfig,
		TLSHandshakeTimeout: 15 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		// The context already bounds the run; a hard client timeout is what
		// lets one hung stream fail instead of holding the WaitGroup open.
		Timeout: maxDuration + 5*time.Second,
	}

	downloadCtx, cancel := context.WithTimeout(ctx, maxDuration)
	defer cancel()

	var downloadedAtomic int64
	var peakBits uint64

	start := time.Now()

	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(peakSampleInterval)
		defer ticker.Stop()

		var lastBytes int64
		lastTime := start

		for {
			select {
			case <-downloadCtx.Done():
				return
			case t := <-ticker.C:
				currentBytes := atomic.LoadInt64(&downloadedAtomic)
				bytesInInterval := currentBytes - lastBytes
				intervalSec := t.Sub(lastTime).Seconds()
				if intervalSec > 0 && bytesInInterval > 0 {
					currentMbps := (float64(bytesInInterval) * 8.0 / 1e6) / intervalSec
					// CAS: several intervals cannot sample simultaneously, but
					// the peak has to beat a strict maximum even in the race
					// case, so this is compare-and-swap rather than a plain
					// store.
					for {
						oldBits := atomic.LoadUint64(&peakBits)
						oldVal := math.Float64frombits(oldBits)
						if currentMbps <= oldVal {
							break
						}
						newBits := math.Float64bits(currentMbps)
						if atomic.CompareAndSwapUint64(&peakBits, oldBits, newBits) {
							break
						}
					}
				}
				lastBytes = currentBytes
				lastTime = t
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, streams)

	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req, err := http.NewRequestWithContext(downloadCtx, "GET", testURL, nil)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			req.Header.Set("User-Agent", "YggSpeedTest-Batch/5.0")
			if customHost != "" {
				req.Host = customHost
			}

			resp, err := client.Do(req)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
				select {
				case errCh <- fmt.Errorf("HTTP status %d", resp.StatusCode):
				default:
				}
				return
			}

			buf := make([]byte, 32*1024)
			for {
				select {
				case <-downloadCtx.Done():
					return
				default:
				}

				if maxBytes > 0 && atomic.LoadInt64(&downloadedAtomic) >= maxBytes {
					cancel()
					return
				}

				n, rErr := resp.Body.Read(buf)
				if n > 0 {
					currTotal := atomic.AddInt64(&downloadedAtomic, int64(n))
					if maxBytes > 0 && currTotal >= maxBytes {
						cancel()
						return
					}
				}
				if rErr != nil {
					if rErr != io.EOF && !errors.Is(rErr, context.Canceled) &&
						!errors.Is(rErr, context.DeadlineExceeded) {
						select {
						case errCh <- rErr:
						default:
						}
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	cancel()
	<-sampleDone

	totalBytes = atomic.LoadInt64(&downloadedAtomic)
	durationSec = time.Since(start).Seconds()

	if totalBytes == 0 {
		// Nothing moved: report the real cause when one is available rather
		// than publishing 0 Mbps as if it were a measurement.
		var firstErr error
		select {
		case firstErr = <-errCh:
		default:
			// The deadline fired with zero bytes: the dial or handshake
			// stalled rather than the transfer failing.
			firstErr = fmt.Errorf("timed out after %v with no data downloaded", maxDuration)
		}
		return 0, 0, 0, durationSec, firstErr
	}

	if durationSec > 0 {
		avgMbps = (float64(totalBytes) * 8.0 / 1e6) / durationSec
	}

	peakMbps = math.Float64frombits(atomic.LoadUint64(&peakBits))
	if peakMbps < avgMbps {
		peakMbps = avgMbps
	}

	return avgMbps, peakMbps, totalBytes, durationSec, nil
}
