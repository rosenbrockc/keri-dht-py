FROM ubuntu:20.04

RUN apt-get update && apt-get install -y \
        dialog apt-utils \
    && apt-get clean \
    && echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt-get update && apt-get install -y software-properties-common \
    && /usr/bin/add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get install -y \
        build-essential pkg-config cmake git wget \
        autotools-dev autoconf curl \
        python3.9 python3.9-dev python3.9-distutils \
        libncurses5-dev libreadline-dev nettle-dev libcppunit-dev \
        libgnutls28-dev libuv1-dev libjsoncpp-dev libargon2-dev \
        libssl-dev libfmt-dev libhttp-parser-dev libasio-dev libmsgpack-dev \
        libsodium-dev \
    && apt-get clean


RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
  && python3.9 get-pip.py \
  && rm get-pip.py \
  && ln -s /usr/bin/python3.9 /usr/bin/python \
  && rm /usr/bin/python3 && ln -s /usr/bin/python3.9 /usr/bin/python3 \
  && ln -s /usr/local/bin/pip3.9 /usr/bin/pip \
  && pip --no-cache-dir install --upgrade pip \
  && pip install cython \
  && rm -rf /var/lib/apt/lists/*


RUN echo "*** Downloading RESTinio ***" \
    && mkdir restinio && cd restinio \
    && wget https://github.com/aberaud/restinio/archive/2c0b6f5e5ba04d7a74e8406a3df1fd433680599d.tar.gz \
    && ls -l && tar -xzf 2c0b6f5e5ba04d7a74e8406a3df1fd433680599d.tar.gz \
    && cd restinio-2c0b6f5e5ba04d7a74e8406a3df1fd433680599d/dev \
    && cmake -DCMAKE_INSTALL_PREFIX=/usr -DRESTINIO_TEST=OFF -DRESTINIO_SAMPLE=OFF \
             -DRESTINIO_INSTALL_SAMPLES=OFF -DRESTINIO_BENCH=OFF -DRESTINIO_INSTALL_BENCHES=OFF \
             -DRESTINIO_FIND_DEPS=ON -DRESTINIO_ALLOW_SOBJECTIZER=Off -DRESTINIO_USE_BOOST_ASIO=none . \
    && make -j8 && make install \
    && cd ../../.. && rm -rf restinio


RUN git clone https://github.com/savoirfairelinux/opendht.git \
	&& cd opendht && mkdir build && cd build \
	&& cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DOPENDHT_PYTHON=On -DOPENDHT_LTO=On && make -j8 && make install \
    && pip install ./python \
	&& cd ../.. && rm -rf opendht

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "keridhtd" ]