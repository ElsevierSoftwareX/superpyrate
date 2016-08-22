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

The contents of the python logging file should look something like this::

    [loggers]
    keys=root,luigi

    [handlers]
    keys=consoleHandler,fileHandler

    [formatters]
    keys=fileFormatter,consoleFormatter

    [logger_root]
    level=INFO
    handlers=consoleHandler

    [logger_luigi]
    level=INFO
    handlers=consoleHandler,fileHandler
    qualname=luigi.interface
    propagate=0

    [handler_consoleHandler]
    class=StreamHandler
    level=WARNING
    formatter=consoleFormatter
    args=(sys.stdout,)

    [handler_fileHandler]
    class=FileHandler
    level=INFO
    formatter=fileFormatter
    args=('logfile.log',)

    [formatter_fileFormatter]
    format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
    datefmt=

    [formatter_consoleFormatter]
    format=%(levelname)s - %(message)s
    datefmt=

Luigi
-----
The luigi configuration file can be placed anywhere and is specified through the
environment variable ``LUIGI_CONFIG_PATH``.

More information about the configuration file can be found in the luigi
documentation_.

Here's an example configuration file::

    [resources]
    postgres=12
    matlab=120
    postgres_linuxbox=100

    [core]
    email-type=html
    error-email=user@ucl.ac.uk
    default-scheduler-host=123.45.678.910
    default-scheduler-port=8028
    logging_conf_file=/home/username/logging.conf

    [worker]
    keep_alive=False
    ping_interval=30

.. _documentation: http://luigi.readthedocs.io/en/stable/configuration.html

Postgres
--------
The postgres configuration file ``postgresql.conf`` resides in the data folder
specified by the environment variable ``PGDATA``.

This file contains a number of server specific settings which dramatically affect
the performance of the database, particular for memory and processor intensive
operations such as index generation, clustering and bulk copying of data.

There is a lot of information on how to tweak these settings available online.

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
    default-scheduler-host=123.45.678.910
    default-scheduler-port=1234
    logging_conf_file=/home/ucftxyz/logging.conf

    [worker]
    keep_alive=False
    ping_interval=30

.. _installation:

Installation
============

Non-UCL Users
-------------

To install superpyrate, its necessary to install Postgresql,
and the superpyrate package.

Instructions for how to install `Postgresql`_ can be found on the website.

.. _Postgresql: https://www.postgresql.org/download/

You'll also need an install of Python 3.x.

UCL Users
---------

Using Legion (UCL's high-performance computing assets),
loading Postgresql is as simple as loading the required modules:

If you have `legion-scripts`_ installed,
then you can just run the ``load_postgres.sh`` script.

.. _legion-scripts: https://github.com/UCL-ShippingGroup/legion_scripts.git

Otherwise, we assume install on Linux.  For MacOSx, Windows and other architectures,
refer to the packages and documentation for Postgresql available on the website.
