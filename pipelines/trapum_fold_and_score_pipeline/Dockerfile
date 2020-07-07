# Copyright (C) 2020 by Prajwal Padmanabh
# Licensed under the Academic Free License version 3.0
# This program comes with ABSOLUTELY NO WARRANTY.
# You are free to modify and redistribute this code as long
# as you do not remove the above attribution and reasonably
# inform receipients that you have modified the original work.

FROM ubuntu:16.04

MAINTAINER : Prajwal Padmanabh "prajwalvp@mpifr-bonn.mpg.de"
#CREDITS: Vivek V K  (Largely based on his pulsar-docker)


RUN rm /bin/sh && ln -s /bin/bash /bin/sh 

# Suppress debconf warnings
ENV DEBIAN_FRONTEND noninteractive

# Create psr user which will be used to run commands with reduced privileges.
RUN adduser --disabled-password --gecos 'unprivileged user' psr && \
    echo "psr:psr" | chpasswd && \
    mkdir -p /home/psr/.ssh && \
    chown -R psr:psr /home/psr/.ssh

# Create space for ssh daemon and update the system
RUN echo 'deb http://us.archive.ubuntu.com/ubuntu trusty main multiverse' >> /etc/apt/sources.list && \
    mkdir /var/run/sshd && \
    apt-get -y check && \
    apt-get -y update && \
    apt-get install -y apt-utils apt-transport-https software-properties-common python-software-properties && \
    apt-get -y update --fix-missing && \
    apt-get -y upgrade 


# Install dependencies
RUN apt-get update
RUN apt-get --no-install-recommends -y install \  

    build-essential \
    autoconf \
    autotools-dev \
    automake \
    pkg-config \
    csh \
    cmake \
    gcc \
    gfortran \
    wget \
    git \
    expect \	
    cvs \
    libcfitsio-dev \
    pgplot5 \
    swig2.0 \    
    python \
    python-dev \
    python-pip \
    python-tk \
    libfftw3-3 \
    libfftw3-bin \
    libfftw3-dev \
    libfftw3-single3 \
    libxml2 \
    libxml2-dev \
    libx11-dev \
    libpng12-dev \
    libpng3 \
    libpnglite-dev \   
    libglib2.0-0 \
    libglib2.0-dev \
    openssh-server \
    xorg \
    openbox \
    libhdf5-10 \
    libhdf5-cpp-11 \
    libhdf5-dev \
    libhdf5-serial-dev \
    libltdl-dev \
    gsl-bin \
    libgsl-dev \
    libgsl2 \
    openssh-server \
    docker.io \
    vim \
    emacs \
    gedit \
    bc \
    sudo \
    curl \
    eog \
    latex2html \
    libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/* 

RUN apt-get -y clean

RUN pip install --upgrade pip 

RUN pip install pip -U && \
    pip install setuptools -U && \
    pip install numpy -U && \
    pip install scipy==0.19.0 -U && \
    pip install matplotlib -U && \
    easy_install scikit-learn==0.12.1 && \
    easy_install theano==0.7 && \
    pip install bson && \
    pip install ipython && \
    pip install scikit-image


RUN apt-get update -y && \
    apt-get --no-install-recommends -y install \
    autogen \
    libtool \
    libltdl-dev	
    

# Install Java

RUN apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install tcsh

RUN apt-get update
RUN apt-get install tcsh


USER psr
# Java
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# PGPLOT
ENV PGPLOT_DIR /usr/lib/pgplot5
ENV PGPLOT_FONT /usr/lib/pgplot5/grfont.dat
ENV PGPLOT_INCLUDES /usr/include
ENV PGPLOT_BACKGROUND white
ENV PGPLOT_FOREGROUND black
ENV PGPLOT_DEV /xs

# Define home, psrhome, OSTYPE and create the directory
ENV HOME /home/psr
ENV PSRHOME /home/psr/software
ENV OSTYPE linux
RUN mkdir -p /home/psr/software
WORKDIR $PSRHOME


RUN wget --no-check-certificate https://www.imcce.fr/content/medias/recherche/equipes/asd/calceph/calceph-2.3.2.tar.gz && \
    tar -xvvf calceph-2.3.2.tar.gz -C $PSRHOME && \
    git clone https://github.com/SixByNine/psrxml.git && \
    git clone https://bitbucket.org/psrsoft/tempo2.git && \
    git clone https://git.code.sf.net/p/tempo/tempo && \
    git clone https://git.code.sf.net/p/psrchive/code psrchive && \
    git clone https://github.com/SixByNine/sigproc.git && \
    git clone https://github.com/ewanbarr/sigpyproc.git && \
    git clone https://git.code.sf.net/p/dspsr/code dspsr && \
    git clone git://git.code.sf.net/p/psrdada/code && \
    git clone https://github.com/straten/epsic.git 




# PGPLOT
ENV PGPLOT_DIR="/usr/lib/pgplot5" \
    PGPLOT_FONT="/usr/lib/pgplot5/grfont.dat" \
    PGPLOT_INCLUDES="/usr/include" \
    PGPLOT_BACKGROUND="white" \
    PGPLOT_FOREGROUND="black" \
    PGPLOT_DEV="/xs"

# calceph
ENV CALCEPH=$PSRHOME"/calceph-2.3.2" \
    PATH=$PATH:$PSRHOME"/calceph-2.3.2/install/bin" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/calceph-2.3.2/install/lib" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/calceph-2.3.2/install/include"
WORKDIR $CALCEPH
RUN ./configure --prefix=$CALCEPH/install --with-pic --enable-shared --enable-static --enable-fortran --enable-thread && \
    make && \
    make check && \
    make install


# psrcat
WORKDIR $PSRHOME
RUN  wget http://www.atnf.csiro.au/people/pulsar/psrcat/downloads/psrcat_pkg.tar.gz && \
    tar -xvf psrcat_pkg.tar.gz -C $PSRHOME 
ENV PSRCAT_FILE=$PSRHOME"/psrcat_tar/psrcat.db" \
    PATH=$PATH:$PSRHOME"/psrcat_tar"
WORKDIR $PSRHOME/psrcat_tar
RUN /bin/bash makeit

# psrXML
ENV PSRXML=$PSRHOME"/psrxml" \
    PATH=$PATH:$PSRHOME"/psrxml/install/bin" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/psrxml/install/lib" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/psrxml/install/include"
WORKDIR $PSRXML
RUN autoreconf --install --warnings=none
RUN ./configure --prefix=$PSRXML/install && \
    make && \
    make install

# tempo
ENV TEMPO=$PSRHOME"/tempo" \
    PATH=$PATH:$PSRHOME"/tempo/bin"
WORKDIR $PSRHOME/tempo
RUN ./prepare && \
    ./configure --prefix=$PSRHOME/tempo && \
    make && \
    make install

# tempo2
ENV TEMPO2=$PSRHOME"/tempo2/T2runtime" \
    PATH=$PATH:$PSRHOME"/tempo2/T2runtime/bin" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/tempo2/T2runtime/include" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/tempo2/T2runtime/lib"
WORKDIR $PSRHOME/tempo2
# A fix to get rid of: returned a non-zero code: 126.
RUN sync && perl -pi -e 's/chmod \+x/#chmod +x/' bootstrap
RUN ./bootstrap && \
    ./configure --x-libraries=/usr/lib/x86_64-linux-gnu --with-calceph=$CALCEPH/install/lib --enable-shared --enable-static --with-pic F77=gfortran CPPFLAGS="$CPPFLAGS -I"$CALCEPH"/install/include" LDFLAGS="-L"$CALCEPH"/install/lib" && \
    make -j $(nproc) && \
    make install && \
    make plugins-install
WORKDIR $PSRHOME/tempo2/T2runtime/clock
RUN touch meerkat2gps.clk && \
    echo "# UTC(meerkat) UTC(GPS)" > meerkat2gps.clk && \
    echo "#" >> meerkat2gps.clk && \
    echo "50155.00000 0.0" >> meerkat2gps.clk && \
    echo "58000.00000 0.0" >> meerkat2gps.clk


ENV EPSIC=$PSRHOME"/epsic"
WORKDIR $EPSIC/src

RUN ./bootstrap && \
    ./configure --prefix=$PSRHOME"/epsic/install" && make -j $(nproc) && make &&  make install > test

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/epsic/install/lib" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/epsic/install/include/epsic" \
    PATH=$PATH:$PSRHOME"/epsic/install/bin" \
    CPPFLAGS="-I ${PSRHOME}/epsic/install/include/epsic"  \
    CFLAGS="-I ${PSRHOME}/epsic/install/include/epsic" 



# PSRCHIVE
ENV PSRCHIVE=$PSRHOME"/psrchive/install" \
    PATH=$PATH:$PSRHOME"/psrchive/install/bin" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/psrchive/install/include" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/psrchive/install/lib" \
    PYTHONPATH=$PYTHONPATH:$PSRHOME"/psrchive/install/lib/python2.7/site-packages"


WORKDIR $PSRHOME/psrchive/

RUN ./bootstrap && \
    ./configure --prefix=$PSRCHIVE --x-libraries=/usr/lib/x86_64-linux-gnu --with-psrxml-dir=$PSRXML/install --enable-shared --enable-static F77=gfortran LDFLAGS="-L"$PSRXML"/install/lib" LIBS="-lpsrxml -lxml2" && \
    make -j $(nproc) && \
    make install 


WORKDIR $HOME
RUN $PSRCHIVE/bin/psrchive_config >> .psrchive.cfg && \
    sed -i 's/# ArrivalTime::default_format = Parkes/ArrivalTime::default_format = Tempo2/g' .psrchive.cfg && \
    sed -i 's/# Predictor::default = polyco/Predictor::default = tempo2/g' .psrchive.cfg && \
    sed -i 's/# Predictor::policy = ephem/Predictor::policy = default/g' .psrchive.cfg && \
    sed -i 's/# WeightedFrequency::round_to_kHz = 1/WeightedFrequency::round_to_kHz = 0/g' .psrchive.cfg


# sigpyproc
ENV SIGPYPROC=$PSRHOME"/sigpyproc" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/sigpyproc/lib/c"
WORKDIR $PSRHOME/sigpyproc
RUN python setup.py install --record list.txt --user


# DAL
USER root
WORKDIR $PSRHOME
RUN git clone https://github.com/nextgen-astrodata/DAL.git
ENV DAL=$PSRHOME"/DAL" \
    PATH=$PATH:$PSRHOME/DAL"/install/bin" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME/DAL"/install/include" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME/DAL"/install/lib"
WORKDIR $DAL
RUN mkdir build
WORKDIR $DAL/build
RUN cmake .. -DCMAKE_INSTALL_PREFIX=$DAL/install && \
    make -j $(nproc) && \
    make && \
    make install


ENV PSRDADA=$PSRHOME"/psrdada" \
    PATH=$PATH:$PSRHOME"/psrdada/install/bin" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/psrdada/install/lib" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/psrdada/install/include" \
    CPPFLAGS="$CPPFLAGS -I ${PSRHOME}/psrdada/install/include -I ${PSRHOME}/epsic/install/include/epsic"  \
    CFLAGS="$CFLAGS -I ${PSRHOME}/psrdada/install/include -I ${PSRHOME}/epsic/install/include/epsic" 

#psrdada
RUN     mv $PSRHOME/code $PSRDADA
WORKDIR $PSRDADA
RUN ./bootstrap && ./configure --prefix=$PSRDADA/install --enable-shared && \
        make -j $(nproc) &&\
        make &&\
        make install

ENV PATH=$PATH:"$PSRDADA/install/bin"


# h5check
WORKDIR $PSRHOME
RUN wget https://www.hdfgroup.org/ftp/HDF5/tools/h5check/src/h5check-2.0.1.tar.gz && \
    tar -xvvf h5check-2.0.1.tar.gz 
ENV H5CHECK=$PSRHOME"/h5check-2.0.1" \
    PATH=$PATH:$PSRHOME"/h5check-2.0.1/install/bin"
WORKDIR $H5CHECK
RUN ./configure --prefix=$H5CHECK/install && \
    make && \
    make install

# DSPSR
ENV DSPSR=$PSRHOME"/dspsr" \
    PATH=$PATH:$PSRHOME"/dspsr/install/bin" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/dspsr/install/lib" \
    C_INCLUDE_PATH=$C_INCLUDE_PATH:$PSRHOME"/dspsr/install/include"

WORKDIR $DSPSR
RUN ./bootstrap && \
    echo "apsr asp bcpm bpsr caspsr cpsr cpsr2 dada dummy fits kat lbadr lbadr64  lump lwa puma2 sigproc ska1" > backends.list && \
    ./configure --prefix=$DSPSR/install --x-libraries=/usr/lib/x86_64-linux-gnu CPPFLAGS="$CPPFLAGS -I"$DAL"/install/include  -I"$PSRXML"/install/include" LDFLAGS="-L"$DAL"/install/lib  -L"$PSRXML"/install/lib -L/usr/local/cuda/lib64" LIBS="-lpgplot -lcpgplot -lpsrxml -lxml2" && \
    make -j $(nproc) && \
    make && \
    make install


# PRESTO 3
WORKDIR $PSRHOME
RUN git clone https://github.com/scottransom/presto.git 
ENV PRESTO=$PSRHOME"/presto" \
    PATH=$PATH:$PSRHOME"/presto/bin" \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PSRHOME"/presto/lib" \
    PYTHONPATH=$PYTHONPATH:$PSRHOME"/presto/lib/python"
WORKDIR $PRESTO/src
#RUN make makewisdom
RUN make prep && \
    make -j $(nproc) && \
    make 
#    make mpi
WORKDIR $PRESTO
RUN pip install .
ENV PYTHONPATH $PYTHONPATH:/home/psr/software/presto/python/presto

RUN env | awk '{print "export ",$0}' >> $HOME/.profile

# PICS 
WORKDIR $HOME
RUN git clone https://github.com/zhuww/ubc_AI.git

WORKDIR $HOME/ubc_AI



# TRAPUM utilities 

RUN apt-get install -y build-essential python3 python3-dev python3-pip
RUN pip3 install --upgrade pip
RUN pip3 install numpy 
RUN pip2 install clfd 
RUN pip3 install xxhash
RUN pip3 install --upgrade setuptools
RUN pip3 install mysqlclient
RUN pip3 install pika
RUN pip3 install pymysql 
RUN pip3 install sqlalchemy
RUN pip3 install sqlacodegen


WORKDIR $PSRHOME
RUN rm -rf ./*.bz2 ./*.gz ./*.xz ./*.ztar ./*.zip
RUN git clone https://github.com/MPIfR-BDG/trapum-pipeline-wrapper.git



# !! Add folding+scoring pipeline routine function to test_receive
