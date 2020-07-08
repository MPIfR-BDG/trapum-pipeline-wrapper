FROM nvidia/cuda:8.0-devel-ubuntu16.04


RUN apt-get update &&\
    apt-get install -y --no-install-recommends \
    git \   
    ca-certificates

WORKDIR /software/

RUN git clone https://github.com/ewanbarr/dedisp.git && \
    cd dedisp &&\
    git checkout arch61 &&\
    make -j 32 && \
    make install 

RUN git clone https://github.com/prajwalvp/peasoup_32.git && \
    cd peasoup_32 && \
    make -j 32 && \
    make install 
   
RUN ldconfig /usr/local/lib


RUN apt-get install -y --no-install-recommends build-essential git curl wget make cmake fftw3 fftw3-dev pkg-config libomp-dev libmysqlclient-dev numactl

#RUN pip install pika
#RUN pip install mysqlclient

# Install sigpyproc
#RUN git clone https://github.com/ewanbarr/sigpyproc.git
#WORKDIR /software/sigpyproc
#RUN python setup.py install

# Python3.6 stuff
RUN apt-get update && \
  apt-get install -y software-properties-common && \
  add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update

RUN apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
RUN apt-get install -y git

# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install numpy matplotlib 

# Install sigpyproc3
WORKDIR /software/
RUN pip install git+https://github.com/pravirkr/sigpyproc3


# TRAPUM utilities

RUN pip install xxhash && \
    pip install pika && \
    pip install sqlalchemy  && \
    pip install pymysql && \
    pip install sqlacodegen && \
    pip install mysqlclient 

RUN git clone https://github.com/MPIfR-BDG/trapum-pipeline-wrapper.git && \
    cd trapum-pipeline-wrapper && \
    git checkout peasoup32_wrapper 


# IQRM 

RUN apt-get install -y libboost-all-dev
WORKDIR /software/
RUN git clone https://gitlab.com/kmrajwade/iqrm_apollo.git && \
    cd iqrm_apollo/ && \
    mkdir build && \
    cd build && \ 
    cmake -DBOOST_ROOT=/ ../ && \
    make -j
ENV PATH $PATH:/software/iqrm_apollo/build/iqrm_apollo


# ft_scrunch 
RUN pip3 install pyyaml
RUN pip3 install scipy
RUN pip3 install git+https://bitbucket.org/mkeith/filtools


# Mongo DB stuff
RUN pip3 install lxml
RUN pip3 install pymongo
RUN pip3 install xmljson


# Extras
RUN apt-get -y install libgfortran3

WORKDIR /software/trapum-pipeline-wrapper/pipelines/trapum_search_pipeline

CMD ["bash"]
