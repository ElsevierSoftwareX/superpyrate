#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Tests the three tasks in the prototype pipeline
"""
import pytest
from superpyrate.pipeline import ClusterAisClean
from conftest import set_env_vars, setup_clean_db, setup_working_folder
import luigi


__author__ = "Will Usher"
__copyright__ = "Will Usher"
__license__ = "mit"

class TestWholePipeline():
    """
    """
    def test_whole_pipeline_runs(self, setup_clean_db,
                                 set_env_vars,
                                 setup_working_folder):
        """
        """
        task = ClusterAisClean(folder_of_zips='tests/fixtures/testais/',
                               with_db=True)
        assert luigi.build([task], local_scheduler=True)


class TestFileNames():
    """
    """
    def test_filename_parsing(self, set_env_vars):
        """ There's a bunch of different filename formats we're receiving from
        our data provider.  Ensure that we're parsing them correctly, and our
        pipeline doesn't fail due to an unpleasantly named file.
        """
        pass

class TestCopyFrom():
    """ Test successful copy of data from validated csv file to postgres database
    """

    def test_valid_database_ingest(self, set_env_vars):
        """ Test ingest of clean rows from clean csv file into the ais_clean table
        """
        pass

    def test_dirty_database_ingest(self, set_env_vars):
        """ Test ingest of dirty rows from dirty csv file into the ais_dirty table
        """
        pass

    def test_successful_ingest_filename_ais_source(self, set_env_vars):
        """ Once successfully ingested, the filename of the file should populate
        the ais_sources table, along with counts of clean, dirty and invalid rows
        """
        pass

class TestCreateIndices():
    """Test that indices are generated in the test database
    """
    def test_create_indices(self, set_env_vars):
        """
        """
        pass
