#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for superpyrate.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""
from __future__ import print_function, absolute_import, division
from superpyrate.db_setup import main as db_setup
from superpyrate.db_setup import make_options

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
def setup_working_folder(tmpdir):
    tempfilepath = tmpdir.mkdir("working_folder")
    os.environ['LUIGIWORK'] = str(tempfilepath)
    return tempfilepath


@pytest.fixture(scope='function')
def setup_clean_db(request):
    """Sets up and tearsdown the database for tests
    """
    db_setup()
    def fin():
        options = make_options()
        sql = "drop schema public cascade; create schema public;"
        db = AISdb(options)
        with db:
            with db.conn.cursor() as cur:
                cur.execute(sql)
            db.conn.commit()
    request.addfinalizer(fin)
