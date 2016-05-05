.. _setup:

=======
Setup
=======

Requirements

postgresql>9.5

    mkdir postgres
    cd postgres
    wget https://ftp.postgresql.org/pub/source/v9.5.2/postgresql-9.5.2.tar.gz

    gunzip postgresql-9.5.2.tar.gz
    tar xf postgresql-9.5.2.tar

    cd postgresql-9.5.2
    # Install a local version of postgres in the user directory on the login node
    ./configure --prefix=/home/***REMOVED***/local/pgsql

    # make
    # make install

    make world
    # make check
    make install-world

    # Tidy up
    make clean

    # Setup path
    echo 'export PATH=/home/***REMOVED***/local/pgsql/bin:$PATH' >> ~/.bash_profile

    # Initialise the database server using Scratch for the data
    initdb -D ./Scratch/data
    # Start the database server
    pg_ctl -D ./Scratch/data -l logfile start

    # Create the test database
    createdb test_aisdb
    # Use the following command to access the database schema and tables
    #psql --host=localhost --port=5432 --username=***REMOVED*** --dbname=test_aisdb


    # Setup virtual python environment using conda
    wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh \
        -O miniconda.sh
    chmod +x miniconda.sh && ./miniconda.sh -b -p $HOME/miniconda
    export PATH=$HOME/miniconda/bin:$PATH
    conda update --yes conda

    # Configure the conda environment and put it in the path using the
    # provided versions
    conda create -n testenv --yes python=$PYTHON_VERSION pip scipy pandas numpy psycopg2 sphinx pylint
    source activate testenv

    cd ~/
    git clone https://github.com/UCL-ShippingGroup/superpyrate.git
    pip install -r requirements.txt
    python setup.py develop

    cd
