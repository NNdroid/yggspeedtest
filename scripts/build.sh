#!/bin/bash

# 項目名稱（輸出的文件名）
BINARY_NAME="yggspeedtest"
# 原始碼文件
SOURCE_FILE="main.go"
# 輸出目錄
BUILD_DIR="bin"

# 確保輸出目錄存在
mkdir -p $BUILD_DIR

# 預定義要編譯的平台列表 (OS/Arch/Variant)
# 格式: "GOOS/GOARCH/GOARM_or_GOMIPS"
PLATFORMS=(
    "linux/amd64"         # 標準 64 位 Linux 伺服器
    "linux/arm64"         # ARM64 (樹莓派 4/5, 新型手機, AWS Graviton)
    "linux/arm/7"         # ARMv7 (舊版樹莓派, 某些嵌入式設備)
    "linux/mipsle/softfloat" # MIPS Little Endian (常見於 OpenWrt 路由器，如 MT7621)
    "darwin/amd64"        # Intel 芯片的 Mac
    "darwin/arm64"        # Apple Silicon (M1/M2/M3) 的 Mac
    "windows/amd64"       # 64 位 Windows
)

echo "--- 開始交叉編譯 $BINARY_NAME ---"

# 執行 go mod tidy 確保依賴完整
echo "正在檢查依賴..."
go mod tidy

for PLATFORM in "${PLATFORMS[@]}"; do
    # 拆分平台字串
    IFS="/" read -r -a PARTS <<< "$PLATFORM"
    OS=${PARTS[0]}
    ARCH=${PARTS[1]}
    VARIANT=${PARTS[2]}

    # 設置輸出文件名
    OUTPUT_NAME="${BINARY_NAME}-${OS}-${ARCH}"
    
    # 處理特殊的環境變量
    export GOOS=$OS
    export GOARCH=$ARCH
    export CGO_ENABLED=0 # 禁用 CGO 以實現純靜態鏈接，提高移植性

    if [ "$ARCH" == "arm" ] && [ -n "$VARIANT" ]; then
        export GOARM=$VARIANT
        OUTPUT_NAME="${OUTPUT_NAME}v${VARIANT}"
    fi

    if [ "$ARCH" == "mipsle" ] || [ "$ARCH" == "mips" ]; then
        export GOMIPS=$VARIANT
    fi

    # Windows 加上 .exe 後綴
    if [ "$OS" == "windows" ]; then
        OUTPUT_NAME="${OUTPUT_NAME}.exe"
    fi

    echo "正在編譯: $OS/$ARCH ($VARIANT)..."
    
    # 執行編譯指令
    # -s -w: 壓縮體積，移除除錯符號
    go build -ldflags="-s -w" -o "${BUILD_DIR}/${OUTPUT_NAME}" "$SOURCE_FILE"

    if [ $? -eq 0 ]; then
        echo "成功: ${BUILD_DIR}/${OUTPUT_NAME}"
    else
        echo "失敗: $OS/$ARCH"
    fi
done

echo "--- 編譯完成，文件位於 $BUILD_DIR 目錄中 ---"
