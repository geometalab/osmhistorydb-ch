FROM ubuntu:22.04

RUN apt-get update \
   &&  apt-get install -y \
    git \
    wget \
    python3-pip \
    python3 \
    python-is-python3 \
    build-essential \
    cmake \
    libboost-dev \
    libexpat1-dev \
    zlib1g-dev \
    libbz2-dev \
    libboost-all-dev \
    libosmium2-dev \
    osmium-tool \
    postgresql-client \
   &&  rm -rf /var/lib/apt/lists/*

ENV LAST_FORCED_UPDATE=2022-06-27T10:44:04+02:00

WORKDIR /code

RUN git clone https://github.com/geometalab/ChangesetMD.git \
   &&  cd ChangesetMD \
   &&  git checkout py3-and-docker

RUN git clone https://github.com/lbuchli/osm-postgresql-experiments \
   &&  cd osm-postgresql-experiments \
   &&  mkdir build \
   &&  cd build \
   &&  cmake .. \
   &&  make install

RUN apt-get update \
   &&  apt-get install -y  \
   &&  rm -rf /var/lib/apt/lists/*
RUN pip install psycopg2-binary lxml bz2file osmium requests pyyaml

ADD OSM_Objects/ /code/
ADD docker/initialize_data.py /code/
ADD docker/run_forever.py /code/
ADD docker/00_create_nwr_change_view.sql /code/
ADD docker/entrypoint.sh /entrypoint.sh

ENV INSIDE_DOCKER=any-value SEQUENCE_STATE_FILE=/var/cache/osmhistory-replication/initial-data/sequence.state

ENTRYPOINT [ "/entrypoint.sh" ]
CMD [ "python", "run_forever.py" ]
# CMD [ "./insert_expanded.sh" ]
