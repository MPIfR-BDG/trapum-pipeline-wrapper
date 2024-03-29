FROM nvidia/cuda:10.2-devel-ubuntu16.04

# Python3.6 stuff
RUN apt-get update && \
  apt-get install -y software-properties-common && \
  add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update

RUN apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
RUN apt-get install -y git

RUN git clone https://github.com/ewanbarr/dedisp.git && \
    cd dedisp &&\
    git checkout arch61 &&\
    make -j 32 && \
    make install

RUN git clone https://github.com/ewanbarr/peasoup.git && \
    cd peasoup && \
    git checkout dedisp_gulping &&\
    make -j 32 && \
    make install

# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install numpy matplotlib 
  
RUN ldconfig /usr/local/lib


RUN apt-get update && apt-get install -y --no-install-recommends build-essential git curl wget make cmake fftw3 fftw3-dev pkg-config libomp-dev libmysqlclient-dev numactl



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

RUN git clone https://github.com/MPIfR-BDG/trapum-pipeline-wrapper.git 


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


# Add pipeline wrapper to pythonpath
ENV PYTHONPATH $PYTHONPATH:/software/trapum-pipeline-wrapper:/software/trapum-pipeline-wrapper/utils

# Add digifil libraries to library path
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/software/trapum-pipeline-wrapper/utils/digifil_libs

# Add utils to $PATH
ENV PATH $PATH:/software/trapum-pipeline-wrapper/utils

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
