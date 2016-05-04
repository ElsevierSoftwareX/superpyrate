#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Tests the three tasks in the prototype pipeline
"""

import pytest
from superpyrate import pipeline

__author__ = "Will Usher"
__copyright__ = "Will Usher"
__license__ = "mit"

class TestFileNames():
    """
    """
    def test_filename_parsing(self):
        """ There's a bunch of different filename formats we're receiving from
        our data provider.  Ensure that we're parsing them correctly, and our
        pipeline doesn't fail due to an unpleasantly named file.
        """
        pass


class TestGenerationOfValidCsv():
    """
    """
    def test_valid_files_produced(self):
        """ Once we have the files, check that some generated test files are
        correctly ingested, including csv files where commas appear in fields
        (e.g. in vessel names)
        """
        pass

    def test_clean_files_produced(self):
        """ Check that valid data goes into a clean folder
        """
        pass

    def test_dirty_files_produced(self):
        """ Check that data which doesn't pass validation goes into a dirty
        folder
        """
        pass

    def test_invalid_data_triggers_log_entry(self):
        """ Rows which are invalid, i.e. cannot be read, should trigger an
        entry in the logfile
        """
        pass


class TestCopyFrom():
    """ Test successful copy of data from validated csv file to postgres database
    """

    def test_valid_database_ingest(self):
        """ Test ingest of clean rows from clean csv file into the ais_clean table
        """
        pass

    def test_dirty_database_ingest(self):
        """ Test ingest of dirty rows from dirty csv file into the ais_dirty table
        """
        pass

    def test_successful_ingest_filename_ais_source(self):
        """ Once successfully ingested, the filename of the file should populate
        the ais_sources table, along with counts of clean, dirty and invalid rows
        """
        pass
