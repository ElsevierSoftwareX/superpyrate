#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for superpyrate.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""
from __future__ import print_function, absolute_import, division

import pytest
import os
from pyrate.repositories.aisdb import AISdb
from superpyrate.pipeline import get_environment_variable

@pytest.fixture(scope='session')
def set_tmpdir_environment(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('test_aiscsv')
    os.environ['TMPDIR'] = str(tmpdir)
    return tmpdir

@pytest.fixture(scope='session')
def set_env_vars():
    os.environ['DBHOSTNAME'] = 'localhost'
    os.environ['DBNAME'] = 'test_aisdb'
    os.environ['DBUSER'] = 'test_ais'
    os.environ['DBUSERPASS'] = 'test_ais'
    os.environ['zippath'] = '/usr/local/bin/'


@pytest.fixture(scope='function')
def setup_clean_db(tmpdir):
    options = {}
    options['host'] = get_environment_variable('DBHOSTNAME')
    options['db'] = get_environment_variable('DBNAME')
    options['user'] = get_environment_variable('DBUSER')
    options['pass'] = get_environment_variable('DBUSERPASS')

    db = AISdb(options)
    with db:
        db.truncate()
        db.clean.create()

    tempfilepath = tmpdir.mkdir("working_folder")
    os.environ['LUIGIWORK'] = str(tempfilepath)
