from superpyrate.tasks import produce_valid_csv_file
import os
import tempfile
import csv
from pytest import fixture

from pyrate.algorithms.aisparser import readcsv, parse_raw_row, \
                                        AIS_CSV_COLUMNS, \
                                        validate_row

class TestUnicodeError:
    """Given fault, bugridden, error prone files, test that ingest still happens
    """
    def test_unicode_error_in_csv(self, set_tmpdir_environment):
        unicode_error_file = "tests/fixtures/unicode_error.csv"
        expected_output = os.path.join(str(set_tmpdir_environment), 'expected_output.csv')
        with open(unicode_error_file, 'r') as input_file:
            with open(expected_output, 'w') as output_file:
                produce_valid_csv_file(input_file, output_file)

        expected = ""

        with open(output_file, 'r') as actual_file:
            assert actual_file.readline().rstrip('\n\r').split(',') == AIS_CSV_COLUMNS
            assert actual_file.readline().rstrip('\n\r') == expected



class TestGenerationOfValidCsv():
    """
    """

    def test_script_runs(self, set_tmpdir_environment):
        """
        """
        input_file = os.path.join(str(set_tmpdir_environment), 'test_input.csv')

        with open(input_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=AIS_CSV_COLUMNS, dialect="excel")
            writer.writeheader()
            writer.writerow({'ETA_minute': '45'})
            writer.writerow({'IMO': 'Baked', 'Navigational_status': '4'})

        output_file = os.path.join(str(set_tmpdir_environment), 'test_output.csv')

        with open(input_file, 'r') as infile:
            with open(output_file, 'w') as outfile:
                produce_valid_csv_file(infile, outfile)

    def test_valid_files_produced(self, set_tmpdir_environment):
        """ Once we have the files, check that some generated test files are
        correctly ingested, including csv files where commas appear in fields
        (e.g. in vessel names)
        """
        input_file = 'tests/fixtures/error.csv'
        output_file = os.path.join(str(set_tmpdir_environment), 'test_output.csv')

        with open(input_file, 'r') as infile:
            with open(output_file, 'w') as outfile:
                produce_valid_csv_file(infile, outfile)

        expected = "355999000,2013-07-15 08:18:57,5,,,,,,,8514083,68.0,JP MKW,PYXIS,7,31,0,0"
        with open(output_file, 'r') as actual_file:
            assert actual_file.readline().rstrip('\n\r').split(',') == AIS_CSV_COLUMNS
            assert actual_file.readline().rstrip('\n\r') == expected

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
