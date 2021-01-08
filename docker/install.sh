#!/usr/bin/env bash

# install leveldb
cd $GOPATH
mkdir dep && cd dep
apt-get update && apt-get -y install cmake

git clone --recurse-submodules https://github.com/google/leveldb.git && cd leveldb
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
cd .. && mkdir lib && cp ./build/libleveldb.a ./lib/ && cd $GOPATH

export CGO_CFLAGS="-I$GOPATH/dep/leveldb/include"
export CGO_LDFLAGS="-L$GOPATH/dep/leveldb/lib"

# install levigo
go get github.com/jmhodges/levigo

# install kvraft
cd $GOPATH/src
git clone https://github.com/sls980/kvraft.git
go install kvraft
go install kvraft/cli