===========
superpyrate
===========

.. image:: https://travis-ci.com/UCL-ShippingGroup/superpyrate.svg?token=zHcMSQsYgUFq9yhr52P7&branch=master
    :target: https://travis-ci.com/UCL-ShippingGroup/superpyrate

Does what pyrate does, but supercharged.


Description
===========

Superpyrate ingests AIS data in parallel to a postgres database, and provides
a framework for running algorithms and tasks on this AIS data.

Superpyrate rests heavily on luigi_, a pipeline and dataflow manager which
uses large directed acyclic graphs model and manage the flows of data between tasks.

.. _luigi: http://luigi.readthedocs.io/en/stable/


Dependencies
============

Postgres 9.5.3
--------------

7zip
----


Installation
============


Configuration
=============
Setup and initialised a remote luigi server to host the scheduler.
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
environment variable `LUIGI_CONFIG_PATH`.

More information about the configuration file can be found in the luigi
documentation_.

.. _documentation: http://luigi.readthedocs.io/en/stable/configuration.html


Postgres
--------
The postgres configuration file `postgresql.conf` resides in the data folder
specified by the environment variable `$PGDATA`.

This file contains a number of server specific settings which dramatically affect
the performance of the database, particular for memory and processor intensive
operations such as index generation, clustering and bulk copying of data.

Various postgres configuration files are stored in a github repository_.

.. _repository: https://gist.github.com/willu47/d4b0f246e4cb4f079b4e415528fbcdd2
