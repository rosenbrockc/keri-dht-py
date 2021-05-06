#!/bin/bash
brew install gnutls msgpack
brew install asio

# Uncomment for peer discovery or HTTP support.
#brew install fmt jsoncpp

git clone https://github.com/savoirfairelinux/opendht.git
cd opendht

# Install restinio dependency for peer discovery and HTTP support.
# mkdir restinio
# cd restinio
# wget https://github.com/aberaud/restinio/archive/2c0b6f5e5ba04d7a74e8406a3df1fd433680599d.tar.gz
# tar -xzf 2c0b6f5e5ba04d7a74e8406a3df1fd433680599d.tar.gz
# cd restinio-2c0b6f5e5ba04d7a74e8406a3df1fd433680599d/dev
# cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DRESTINIO_TEST=OFF -DRESTINIO_SAMPLE=OFF \
#     -DRESTINIO_INSTALL_SAMPLES=OFF -DRESTINIO_BENCH=OFF -DRESTINIO_INSTALL_BENCHES=OFF \
#     -DRESTINIO_FIND_DEPS=ON -DRESTINIO_ALLOW_SOBJECTIZER=Off -DRESTINIO_USE_BOOST_ASIO=none .
# cd ../../.. && rm -rf restinio

mkdir build && cd build
`brew info openssl | grep PKG_CONFIG_PATH`
cmake -DOPENDHT_PYTHON=ON -DCMAKE_INSTALL_PREFIX=/usr/local ..
make -j4
make install

pip install build/python