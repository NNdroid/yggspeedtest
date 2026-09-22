#!/bin/bash

# 交叉編譯 YggSpeedTest 的兩個二進位元到 bin/。
#
#   ./scripts/build.sh                 # 用 engine 內建的預設版本號
#   ./scripts/build.sh v2.1.0          # 指定版本號，寫入二進位元
#
# 兩個產物：
#   yggspeedtest            命令行，一次性批測，輸出表格
#   yggspeedtest-web        Web 儀表板 + 排程執行器（內嵌 UI，離線可用）

set -uo pipefail

BUILD_DIR="bin"

# 格式: "輸出名前綴:包路徑"
BINS=(
    "yggspeedtest:./cmd/yggspeedtest"
    "yggspeedtest-web:./cmd/yggspeedtest-web"
)

# 預定義要編譯的平台列表 (OS/Arch/Variant)
PLATFORMS=(
    "linux/amd64"            # 標準 64 位 Linux 伺服器
    "linux/arm64"            # ARM64 (樹莓派 4/5, AWS Graviton)
    "linux/arm/7"            # ARMv7 (舊版樹莓派, 某些嵌入式設備)
    "linux/mipsle/softfloat" # MIPS Little Endian (OpenWrt 路由器, 如 MT7621)
    "darwin/amd64"           # Intel 芯片的 Mac
    "darwin/arm64"           # Apple Silicon (M1/M2/M3) 的 Mac
    "windows/amd64"          # 64 位 Windows
    "windows/arm64"          # 64 位 Windows on ARM
)

# -X 注入的變量。版本號在 internal/engine 裡，兩個二進位元共用同一份。
VERSION="${1:-}"
LDFLAGS="-s -w"
if [ -n "$VERSION" ]; then
    LDFLAGS="$LDFLAGS -X yggspeedtest/internal/engine.Version=$VERSION"
fi

mkdir -p "$BUILD_DIR"

echo "--- 檢查依賴與型別 ---"
go mod tidy || echo "警告: go mod tidy 失敗，繼續編譯"
go build ./... || { echo "失敗: go build ./... 未通過"; exit 1; }

echo "--- 開始交叉編譯 (${#BINS[@]} 個二進位元 × ${#PLATFORMS[@]} 個平台) ---"

FAILED=0

for BIN in "${BINS[@]}"; do
    NAME=${BIN%%:*}
    PKG=${BIN#*:}

    for PLATFORM in "${PLATFORMS[@]}"; do
        IFS="/" read -r -a PARTS <<< "$PLATFORM"
        OS=${PARTS[0]}
        ARCH=${PARTS[1]}
        VARIANT=${PARTS[2]:-}

        OUTPUT_NAME="${NAME}-${OS}-${ARCH}"

        export GOOS=$OS
        export GOARCH=$ARCH
        export CGO_ENABLED=0  # 純靜態鏈接，提高移植性

        # GOARM / GOMIPS 只對特定架構有意義，其餘平台先清掉，避免上一次循環的殘留。
        unset GOARM GOMIPS
        if [ "$ARCH" == "arm" ] && [ -n "$VARIANT" ]; then
            export GOARM=$VARIANT
            OUTPUT_NAME="${OUTPUT_NAME}v${VARIANT}"
        fi
        if [ "$ARCH" == "mipsle" ] || [ "$ARCH" == "mips" ]; then
            export GOMIPS=$VARIANT
        fi

        if [ "$OS" == "windows" ]; then
            OUTPUT_NAME="${OUTPUT_NAME}.exe"
        fi

        echo "編譯: $NAME $OS/$ARCH ${VARIANT:+($VARIANT)} ..."
        if go build -ldflags="$LDFLAGS" -o "${BUILD_DIR}/${OUTPUT_NAME}" "$PKG"; then
            echo "  成功: ${BUILD_DIR}/${OUTPUT_NAME}"
        else
            echo "  失敗: $NAME $OS/$ARCH"
            FAILED=$((FAILED + 1))
        fi
    done
done

# 復位環境，避免污染後續命令
unset GOOS GOARCH GOARM GOMIPS CGO_ENABLED

echo "--- 編譯完成，文件位於 $BUILD_DIR ---"
ls -1 "$BUILD_DIR"

if [ "$FAILED" -gt 0 ]; then
    echo "有 $FAILED 個目標編譯失敗"
    exit 1
fi
