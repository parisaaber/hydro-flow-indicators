ARG PYTHON_RUNTIME=3.12

FROM public.ecr.aws/lambda/python:${PYTHON_RUNTIME} AS build

ENV PREFIX=$LAMBDA_TASK_ROOT

ENV \
    CMAKE_VERSION=3.31.6 \
    LIBTIFF_VERSION=4.6.0 \
    PROJ_VERSION=9.5.1 \
    LIBGEOTIFF_VERSION=1.7.4 \
    GEOS_VERSION=3.13.1 \
    GDAL_VERSION=3.10.2 \
    LD_LIBRARY_PATH=$PREFIX/lib:$LD_LIBRARY_PATH \
    PATH=$PREFIX/bin:$PATH \
    CPATH=$PREFIX/include \
    PYTHONPATH=$PREFIX/py \
    ARROW_VERSION=19.0.1 \
    GDAL_DATA=$PREFIX/share/gdal \
    PROJ_DATA=$PREFIX/share/proj \
    GDAL_CONFIG=$PREFIX/bin/gdal-config \
    GEOS_CONFIG=$PREFIX/bin/geos-config

RUN dnf install -y \
    wget \
    git-all

RUN cd /tmp \
    && wget https://cmake.org/files/v${CMAKE_VERSION%.*}/cmake-${CMAKE_VERSION}.tar.gz \
    && git clone https://github.com/tzutalin/minizip.git \
    && wget https://github.com/uriparser/uriparser/releases/download/uriparser-0.9.8/uriparser-0.9.8.tar.gz \
    && wget https://github.com/libkml/libkml/archive/refs/tags/1.3.0.tar.gz \
    && wget https://download.osgeo.org/libtiff/tiff-${LIBTIFF_VERSION}.tar.gz \
    && wget https://download.osgeo.org/proj/proj-${PROJ_VERSION}.tar.gz \
    && wget https://github.com/OSGeo/libgeotiff/releases/download/${LIBGEOTIFF_VERSION}/libgeotiff-${LIBGEOTIFF_VERSION}.tar.gz \
    && wget https://github.com/libgeos/geos/archive/refs/tags/${GEOS_VERSION}.tar.gz \
    && wget https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz \
    && wget https://github.com/OSGeo/gdal/releases/download/v${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && wget https://github.com/felt/tippecanoe/archive/refs/tags/${TIPPECANOE_VERSION}.tar.gz


RUN dnf install -y \
    which \
    xz-devel \
    pcre2-devel \
    gcc-gfortran \
    libgomp \
    perl \
    libcurl-devel \
    tar \
    openssl-devel \
    bzip2-devel \
    swig \
    libffi-devel \
    sqlite-devel \
    zlib-devel \
    bison \
    flex \
    libpng-devel \
    libzstd-devel \
    libzstd \
    zstd \
    # For kml
    expat-devel \
    boost \
    gtest-devel \
    doxygen


# cmake
RUN cd /tmp \
    && tar -xvzf cmake-$CMAKE_VERSION.tar.gz \
    && cd cmake-$CMAKE_VERSION \
    && ./bootstrap \
    && make -j$(nproc) --silent && make install

RUN mkdir $PREFIX/py \
    && pip install --upgrade pip && pip install setuptools awscli numpy==2.1 -t $PREFIX/py

# libkml (which requires minizip and uriparser)    
RUN cd /tmp \
    && cd minizip \
    && mkdir build \
    && cd build \
    && cmake \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR:PATH=lib \
    .. \
    && make -j $(nproc) \
    && make install

RUN cd /tmp \
    && tar -xvf uriparser-0.9.8.tar.gz \
    && cd uriparser-0.9.8 \
    && mkdir build \
    && cd build \
    && cmake \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR:PATH=lib \
    .. \
    && make -j $(nproc) \
    && make install

RUN cd /tmp \
    && tar -xvf 1.3.0.tar.gz \
    && cd libkml-1.3.0 \
    && mkdir build \
    && cd build \
    && cmake \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR:PATH=lib \
    .. \
    && make -j $(nproc) \
    && make install

# libtiff-4
RUN cd /tmp \
    && tar -xvf tiff-$LIBTIFF_VERSION.tar.gz \
    && cd tiff-$LIBTIFF_VERSION \
    && ./configure --prefix=$PREFIX \
    && make -j$(nproc) --silent \
    && make install

# PROJ
RUN cd /tmp \
    && tar -xvf proj-$PROJ_VERSION.tar.gz \
    && cd proj-$PROJ_VERSION \
    && mkdir build \
    && cd build \
    && cmake \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR:PATH=lib \
    -DBUILD_TESTING=OFF \
    .. \
    && cmake --build . -- -j $(nproc) \
    && cmake --build . --target install \
    && projsync --system-directory --all

# libgeotiff
RUN cd /tmp \
    && tar -xvf libgeotiff-$LIBGEOTIFF_VERSION.tar.gz \
    && cd libgeotiff-$LIBGEOTIFF_VERSION \
    && ./configure \
    --prefix=$PREFIX \
    --with-proj=$PREFIX \
    --with-zip=yes \
    --with-zlib \
    --with-libtiff=$PREFIX \
    && make -j$(nproc) --silent && make install

# GEOS
RUN cd /tmp \
    && tar -xvf $GEOS_VERSION.tar.gz \
    && cd geos-$GEOS_VERSION \
    && mkdir build \
    && cd build \
    && cmake \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR:PATH=lib \
    .. \
    && cmake --build . -- -j $(nproc) \
    && cmake --build . --target install

# Snappy
RUN cd /tmp \
    && git clone https://github.com/google/snappy.git \
    && cd snappy \
    && git submodule update --init \
    && mkdir build \
    && cd build \
    && cmake ../ \
    && make

# Apache Arrow
RUN cd /tmp \
    && tar -xvf apache-arrow-${ARROW_VERSION}.tar.gz \
    && cd arrow-apache-arrow-${ARROW_VERSION}/cpp \
    && mkdir build && cd build \
    && cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DARROW_PARQUET=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_STATIC=OFF \
    && cmake --build . -- -j$(nproc) VERBOSE=1\
    && cmake --install .

# GDAL
RUN cd /tmp \
    && tar -xvf gdal-$GDAL_VERSION.tar.gz \
    && cd gdal-$GDAL_VERSION \
    && mkdir build \
    && cd build \
    && cmake \
    -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR:PATH=lib \
    -DCMAKE_PREFIX_PATH=$PREFIX \
    -DPROJ_LIBRARY=$PREFIX/lib64/libproj.so \
    -DPROJ_INCLUDE_DIR=$PREFIX/include \
    -DGDAL_USE_TIFF_INTERNAL=OFF \
    -DGDAL_USE_GEOTIFF_INTERNAL=OFF \
    -DBUILD_PYTHON_BINDINGS=ON \
    -DGDAL_USE_ARROW=ON \
    -DOGR_ENABLE_DRIVER_PARQUET=ON \
    .. \
    && cmake --build . -- -j $(nproc) && cmake --build . --target install \
    && pip install gdal==${GDAL_VERSION} -t $PREFIX/py

COPY . /tmp/hydro-flow-indicators

RUN cd /tmp/hydro-flow-indicators \
    && pip install --no-cache-dir . -t $PREFIX/py

# Compile python code
RUN python -m compileall $PREFIX/py -q

FROM public.ecr.aws/lambda/python:${PYTHON_RUNTIME}

ENV PREFIX=$LAMBDA_TASK_ROOT
ENV \
    LD_LIBRARY_PATH=$PREFIX/lib:$LD_LIBRARY_PATH \
    PATH=$PREFIX/bin:$PREFIX/py/bin:$PATH \
    PYTHONPATH=$PREFIX/py \
    GDAL_DATA=$PREFIX/share/gdal \
    PROJ_DATA=$PREFIX/share/proj \
    GDAL_CONFIG=$PREFIX/bin/gdal-config \
    GEOS_CONFIG=$PREFIX/bin/geos-config

# All compiled libs
COPY --from=build $PREFIX/lib/ $PREFIX/lib/
COPY --from=build $PREFIX/include/ $PREFIX/include/
COPY --from=build $PREFIX/share/ $PREFIX/share/
COPY --from=build $PREFIX/bin/ $PREFIX/bin/
COPY --from=build $PREFIX/py $PREFIX/py
