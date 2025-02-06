# DINGOFS

## What's dingofs

DingoFS is a project fork from Curve.

Curve is a sandbox project hosted by the CNCF Foundation. It's cloud-native, high-performance, and easy to operate. Curve is an open-source distributed storage system for block and shared file storage.

## Requirements

gcc 13

[dingo-eureka](https://github.com/dingodb/dingo-eureka)

## Dependencies

### Rocky 8.9/9.3

```sh
sudo dnf install -y epel-release
sudo dnf install -y gcc-toolset-13* fuse3-devel  libnl3-devel libunwind-devel python3-devel

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ &&   sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64

source /opt/rh/gcc-toolset-13/enable
```

### Ubuntu 22.04/24.04

```sh
sudo apt update
sudo apt install -y make gcc g++ libnl-genl-3-dev libunwind-dev libfuse3-dev python3-dev

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ && sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64
```

## How to build

### Build dingo-eureka

refer https://github.com/dingodb/dingo-eureka

### Build dingo-sdk

refer https://github.com/dingodb/dingo-sdk

### Install jemalloc
```shell
wget https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
tar -xjvf jemalloc-5.3.0.tar.bz2
cd jemalloc-5.3.0 && ./configure && make && make install
```

### Download dep

```sh
git submodule sync
git submodule update --init --recursive
```

### Build Etcd Client

```sh
bash build_thirdparties.sh
```

### Build
```sh
mkdir build
cd build
cmake ..
make -j 32
```
