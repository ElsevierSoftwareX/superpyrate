import luigi
from luigi import six, postgres
from luigi.util import inherits
from luigi.contrib.sge import SGEJobTask as SGEJobTask
from luigi.contrib.external_program import ExternalProgramTask as ExternalProgramTask
from pyrate.algorithms.aisparser import readcsv, parse_raw_row, AIS_CSV_COLUMNS, validate_row
from pyrate.repositories.aisdb import AISdb
from superpyrate.tasks import produce_valid_csv_file
import csv
import datetime
import psycopg2
import logging
import tempfile
import os
logger = logging.getLogger('luigi-interface')

class Pipeline_Unzip(luigi.WrapperTask):
    """ Runs tasks necessary to unzip and extract CSV files using 7zip
    """
    zip_path = luigi.Parameter()

    def requires(self):
        yield [UnzipFiles(in_file, self.zip_path) for in_file in os.listdir(self.zip_path) if in_file.endswith('.zip')]

class LocateZipFiles(luigi.ExternalTask):

    in_file = luigi.Parameter()
    zip_path = luigi.Parameter()

    def output(self):
        return luigi.file.LocalTarget(self.zip_path + '/' + self.in_file)

class UnzipFiles(ExternalProgramTask):

    in_file = luigi.Parameter()
    zip_path = luigi.Parameter()
    shell_script = luigi.Parameter(default='./unzip_csvs.sh',significant=False)

    def requires(self):
        yield LocateZipFiles(self.in_file,self.zip_path)

    def program_args(self):
        out_dir = os.path.dirname(self.zip_path) + '/unzipped/'
        logger.debug('{0}, {1}, {2}'.format(self.shell_script, os.path.join(os.path.abspath(self.zip_path),self.in_file), os.path.abspath(out_dir)))
        return [self.shell_script, os.path.join(os.path.abspath(self.zip_path),self.in_file), os.path.abspath(out_dir)]

    def output(self):
        out_zip_dir = os.path.abspath(os.path.dirname(self.zip_path) + '/unzipped/' + os.path.splitext(self.in_file)[0])
        logger.debug('Output zip directory: {}'.format(out_zip_dir))
        return luigi.file.LocalTarget(out_zip_dir)


