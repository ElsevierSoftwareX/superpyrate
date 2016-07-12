.. _setup:

=======
Setup
=======

Configuration
=============

After installation_, it is necessary to conduct three separate configuration
tasks for:

* logging
* luigi
* postgres

Setup and initialise a remote luigi server to host the scheduler.
In the UCL shipping group, this is currently hosted on the 'linux box'

Logging
-------
The log output of the superpyrate pipeline is extremely verbose when set to
DEBUG level.  For production, it is strongly recommended that the level is set
to INFO only to avoid the generation of GB-scale log files.

This is easily done using a python logging configuration file, and referencing
the path to the logging config in the luigi configuration file.

Luigi
-----
The luigi configuration file can be placed anywhere and is specified through the
environment variable ``LUIGI_CONFIG_PATH``.

More information about the configuration file can be found in the luigi
documentation_.

.. _documentation: http://luigi.readthedocs.io/en/stable/configuration.html

Postgres
--------
The postgres configuration file ``postgresql.conf`` resides in the data folder
specified by the environment variable ``PGDATA``.

This file contains a number of server specific settings which dramatically affect
the performance of the database, particular for memory and processor intensive
operations such as index generation, clustering and bulk copying of data.

Various postgres configuration files are stored in a github `repository`_.

.. _repository: https://gist.github.com/willu47/d4b0f246e4cb4f079b4e415528fbcdd2

Setup for ingest
================

Setting up the database::

    # Initialise the database server using Scratch for the data
    initdb -D $HOME/Scratch/data
    # Spin up the database server
    pg_ctl -D $HOME/Scratch/data -l logfile start

    # Create the test database
    createdb test_aisdb
    # Use the following command to access the database schema and tables
    #psql --host=localhost --port=5432 --username=test_ais --dbname=test_aisdb

    psql -U postgres -c "create extension postgis"
    psql -c "create database test_aisdb;" -U postgres
    psql -U postgres -c "CREATE USER test_ais WITH PASSWORD 'test_ais' SUPERUSER;"
    psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE test_aisdb to test_ais;"


Setup virtual python environment using conda::

    wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh \
    -O miniconda.sh
    chmod +x miniconda.sh && ./miniconda.sh -b -p $HOME/miniconda
    export PATH=$HOME/miniconda/bin:$PATH
    conda update --yes conda

    # Configure the conda environment and put it in the path using the
    # provided versions
    conda create -n testenv --yes python=$PYTHON_VERSION pip scipy pandas numpy psycopg2 sphinx pylint
    source activate testenv

Before installing superpyrate, you'll need to setup your git account.  Enter the
following commands::

    git config --global user.name "YOUR NAME"
    git config --global user.email "YOUR EMAIL ADDRESS"

To access git from Legion, you'll need to setup a certificate and ssh access to
git.  You can follow the instructions `here`_::

    cd $HOME
    git clone https://github.com/UCL-ShippingGroup/superpyrate.git
    cd superpyrate
    pip install -r requirements.txt
    python setup.py develop

.. _here: https://help.github.com/articles/set-up-git/#platform-linux

It is also a good idea to get the legion-scripts which help with running
superpyrate::

    cd $HOME
    git clone https://github.com/UCL-ShippingGroup/legion-scripts.git

Add a configuration file for py:mod:`luigi`::

    [core]
    default-scheduler-host=***REMOVED***
    default-scheduler-port=8028
    logging_conf_file=/home/ucftxyz/logging.conf

    [worker]
    keep_alive=False
    ping_interval=30

.. _installation:

Installation
============

Using Legion, loading postgres is as simple as loading the required modules:

If you have `legion-scripts`_ installed,
then you can just run the ``load_postgres.sh`` script.

.. _legion-scripts: https://github.com/UCL-ShippingGroup/legion_scripts.git

.. _compiling:

Compiling from source
=====================

First, install postgres::

    cd $HOME
    wget https://ftp.postgresql.org/pub/source/v9.5.2/postgresql-9.5.2.tar.gz
    gunzip postgresql-9.5.2.tar.gz
    tar xf postgresql-9.5.2.tar
    cd postgresql-9.5.2
    # Install a local version of postgres in the user directory on the login node
    ./configure --prefix=$HOME/pgsql
    ./make -s
    ./make install
    ./make clean
    # Setup path
    echo 'PATH=$HOME/pgsql/bin:$PATH' >> ~/.bash_profile


Installing postgis, is painful::

    # Obtain, compile and install postgis and its requirements (GEOS, PROJ4, GDAL)
    cd $HOME
    svn checkout http://svn.osgeo.org/geos/trunk geos-svn
    cd geos-svn
    ./autogen.sh
    ./configure --prefix=$HOME/geos
    ./make -s
    ./make install
    echo 'PATH=$HOME/geos/bin:$PATH' >> ~/.bash_profile
    echo 'export LD_LIBRARY_PATH=$HOME/geos/lib:$LD_LIBRARY_PATH' >> ~/.bash_profile


    cd $HOME
    wget http://download.osgeo.org/proj/proj-4.9.1.tar.gz
    tar xf proj-4.9.1.tar.gz
    cd proj-4.9.1
    ./configure --prefix=$HOME/proj4
    ./make
    ./make install
    echo 'PATH=$HOME/proj4/bin:$PATH' >> ~/.bash_profile
    echo 'export LD_LIBRARY_PATH=$HOME/proj4/lib:$LD_LIBRARY_PATH' >> ~/.bash_profile


    cd $HOME
    wget http://download.osgeo.org/gdal/2.1.0/gdal-2.1.0.tar.gz
    tar xf gdal-2.1.0.tar.gz
    cd gdal-2.1.0
    ./configure --prefix=$HOME/gdal
    ./make
    ./make install
    echo 'export PATH=/$HOME/gdal/bin:$PATH' >> ~/.bash_profile
    echo 'export LD_LIBRARY_PATH=$HOME/gdal/lib:$LD_LIBRARY_PATH' >> ~/.bash_profile
    echo 'export GDAL_DATA=$HOME/gdal/share/gdal' >> ~/.bash_profile
    echo 'export PATH' >> ~/.bash_profile
    # Test
    #% gdalinfo --version
    # See below for installation of Python bindings


    wget http://download.osgeo.org/postgis/source/postgis-2.2.2.tar.gz
    tar xf postgis-2.2.2.tar.gz
    cd postgis-2.2.2
    ./configure --prefix=$HOME/postgis
    ./make
    ./make install
