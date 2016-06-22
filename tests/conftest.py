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

@pytest.fixture(scope='session')
def set_tmpdir_environment(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('test_aiscsv')
    os.environ['TMPDIR'] = str(tmpdir)
    return tmpdir

@pytest.fixture(scope='session')
def set_env_vars():
    os.environ['DBHOSTNAME'] = 'localhost'
    database = os.environ['DBNAME'] = 'test_aisdb'
    os.environ['DBUSER'] = 'postgres'
    os.environ['DBUSERPASS'] = ''
