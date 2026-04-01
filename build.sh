#!/bin/bash
set -e

export VCPKG_ROOT="/Users/liuyifeng/vcpkg"

# 确保 VCPKG_ROOT 环境变量已经生效，避免路径为空导致找不到 vcpkg.cmake
if [ -z "$VCPKG_ROOT" ]; then
    echo "错误: 环境变量 VCPKG_ROOT 未设置或为空。"
    echo "请在终端执行: export VCPKG_ROOT=\"/你的/vcpkg/实际/路径\"，然后再运行此脚本。"
    exit 1
fi

# 让 vcpkg 优先使用 Homebrew 安装的最新版 bison 和 flex
export PATH="/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:$PATH"
export LDFLAGS="-L/opt/homebrew/opt/bison/lib -L/opt/homebrew/opt/flex/lib"
export CPPFLAGS="-I/opt/homebrew/opt/bison/include -I/opt/homebrew/opt/flex/include"

# 创建构建目录
mkdir -p build
# 进入构建目录
# 执行CMake构建，并生成compile_commands.json文件
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build --config Release

echo "===========================构建完成==========================="
# 拷贝可执行文件到项目根目录
cp build/feature_store feature_store
echo "===========================拷贝完成==========================="