#!/bin/bash
set -e

# remove existing build directory
if [ -d "build" ]; then
    echo "Cleaning old build directory..."
    rm -rf build
fi

# create build directory
mkdir build
cd build

# run cmake
echo ""
echo "Running CMake..."
cmake ..

# compile
echo ""
echo "Compiling..."

# install
echo ""
echo "Installing"
sudo make install

echo ""
echo "=========================================="
echo "✅ Installation Complete"
echo "=========================================="
