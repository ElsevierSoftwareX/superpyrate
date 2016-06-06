""" Runs an integrated pipeline from raw zip file to database tables.  This mega-pipeline is constructed
out of three sub-pipelines.

1. Unzip individual AIS archives and output the csv files
2. Validate each of the csv files, processing using a derived version of the pyrate code,
   outputting vaidated csv files
3. Using the postgres `copy` command, ingest the data directly into the database
"""
import luigi
from luigi import six, postgres
from luigi.util import inherits
from luigi.contrib.sge import SGEJobTask as SGEJobTask
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




class Pipeline(luigi.WrapperTask):
    """Wrapper task which performs the entire ingest pipeline
    """
    # Pass in folder with CSV files to parse into Database when calling luigi from the command line: luigi --module superpyrate.pipeline Pipeline --aiscsv-folder ./aiscsv --local-scheduler --workers=2
    source_path = luigi.Parameter(default='./', significant=False)

    def requires(self):
        yield [LoadCleanedAIS(in_file, os.path.abspath(self.source_path)) for in_file in os.listdir(self.source_path) if in_file.endswith('.csv')]

class Pipeline_Valid_Messages(luigi.WrapperTask):
    """This wrapper just runs the validate messages task
    """
    source_path = luigi.Parameter()

    def requires(self):
        yield [ValidMessages(in_file, self.source_path) for in_file in os.listdir(self.source_path) if in_file.endswith('.csv')]

class SourceFiles(luigi.ExternalTask):

    in_file = luigi.Parameter()
    source_path = luigi.Parameter()

    def output(self):
        return luigi.file.LocalTarget(self.source_path + '/' + self.in_file)

class ValidMessages(luigi.Task):
    """ Takes AIS messages and runs validation functions, generating valid csv
    files in folder called 'cleancsv' at the same level as source_path
    """
    in_file = luigi.Parameter()
    source_path = luigi.Parameter()

    def requires(self):
        return SourceFiles(self.in_file, self.source_path)

    def run(self):
        with self.input().open('r') as infile:
            with self.output().open('w') as outfile:
                produce_valid_csv_file(infile, outfile)

    def output(self):
        clean_file_out = os.path.dirname(self.source_path) + '/cleancsv/' + self.in_file
        return luigi.file.LocalTarget(clean_file_out)

class ValidMessagesToDatabase(luigi.postgres.CopyToTable):

    in_file = luigi.Parameter()
    source_path = luigi.Parameter()

    null_values = (None,"")
    column_separator = ","

    host = "localhost"
    database = "test_aisdb"
    user = "postgres"
    password = ""
    table = "ais_clean"

    cols = ['MMSI','Time','Message_ID','Navigational_status','SOG',
               'Longitude','Latitude','COG','Heading','IMO','Draught',
               'Destination','Vessel_Name',
               'ETA_month','ETA_day','ETA_hour','ETA_minute']
    columns = [x.lower() for x in cols]
    # logger.debug("Columns: {}".format(columns))

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                yield row
                # logger.debug(line)
                # yield [x for x in line.strip('\n').split(',') ]

    def requires(self):
        return ValidMessages(self.in_file, self.source_path)

    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))
        logger.debug(self.columns)
        sql = "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(self.table, ",".join(self.columns), file)

        cursor.copy_expert(sql, file)

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()

        with self.input().open('r') as csvfile:
            for attempt in range(2):
                try:
                    cursor = connection.cursor()
                    # self.init_copy(connection)
                    self.copy(cursor, csvfile)
                    # self.post_copy(connection)
                except psycopg2.ProgrammingError as e:
                    if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and attempt == 0:
                        # if first attempt fails with "relation not found", try creating table
                        logger.info("Creating table %s", self.table)
                        connection.reset()
                        self.create_table(connection)
                    else:
                        raise
                else:
                    break

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()

class LoadCleanedAIS(luigi.postgres.CopyToTable):
    """
    Execute ValidMessagesToDatabase and update ais_sources table with name of CSV processed
    """

    in_file = luigi.Parameter()
    source_path = luigi.Parameter()

    null_values = (None,"")
    column_separator = ","

    host = "localhost"
    database = "test_aisdb"
    user = "postgres"
    password = ""
    table = "ais_sources"

    def requires(self):
        return ValidMessagesToDatabase(self.in_file, self.source_path)

    def run(self):
        # Prepare source data to add to ais_sources
        source_data = {'filename':self.in_file,'ext':os.path.splitext(self.in_file)[1],'invalid':0,'clean':0,'dirty':0,'source':0}
        columns = '(' + ','.join([c.lower() for c in source_data.keys()]) + ')'

        connection = self.output().connect()
        cursor = connection.cursor()
        with cursor:
            tuplestr = "(" + ",".join("%({})s".format(i) for i in source_data.keys()) + ")"
            cursor.execute("INSERT INTO " + self.table + " "+ columns + " VALUES "+ tuplestr, source_data)

        # mark as complete
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()



class Pipeline_CopyToDB(luigi.WrapperTask):
    """ Runs tasks necessary to copy cleaned CSVs to the database
    """
    clean_path = luigi.Parameter()

    def requires(self):
        yield [UpdateAISSources(in_file, self.clean_path) for in_file in os.listdir(self.clean_path) if in_file.endswith('.csv')]

class CleanFiles(luigi.ExternalTask):

    in_file = luigi.Parameter()
    clean_path = luigi.Parameter()

    def output(self):
        return luigi.file.LocalTarget(self.clean_path + '/' + self.in_file)

class ValidMessagesToDatabase(luigi.postgres.CopyToTable):

    in_file = luigi.Parameter()
    clean_path = luigi.Parameter()

    null_values = (None,"")
    column_separator = ","

    # host = luigi.Parameter(default='localhost',significant=False)
    # database = luigi.Parameter(default='test_aisdb',significant=False)
    # user = luigi.Parameter(default='postgres',significant=False)
    # password = luigi.Parameter(default='',significant=False)
    # table = luigi.Parameter(default='ais_clean',significant=False)

    host = "localhost"
    database = "pyrate_2015"
    user = "postgres"
    password = ""
    table = "ais_clean"

    # options = {'host':'localhost','db':'pyrate_2015','user':'postgres','pass':''}

    cols = ['MMSI','Time','Message_ID','Navigational_status','SOG',
               'Longitude','Latitude','COG','Heading','IMO','Draught',
               'Destination','Vessel_Name',
               'ETA_month','ETA_day','ETA_hour','ETA_minute']
    columns = [x.lower() for x in cols]
    # logger.debug("Columns: {}".format(columns))

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                yield row
                # logger.debug(line)
                # yield [x for x in line.strip('\n').split(',') ]

    def requires(self):
        # return CreateTables(), CleanFiles(self.in_file, self.clean_path)
        return CleanFiles(self.in_file, self.clean_path)

    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))
        logger.debug(self.columns)
        sql = "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(self.table, ",".join(self.columns), file)

        cursor.copy_expert(sql, file)

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """

        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()

        with self.input().open('r') as csvfile:
            for attempt in range(2):
                try:
                    cursor = connection.cursor()
                    # self.init_copy(connection)
                    self.copy(cursor, csvfile)
                    # self.post_copy(connection)
                except psycopg2.ProgrammingError as e:
                    if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and attempt == 0:
                        # if first attempt fails with "relation not found", try creating table
                        logger.info("Creating table %s", self.table)
                        connection.reset()
                        self.create_table(connection)
                    else:
                        raise
                else:
                    break

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()

class UpdateAISSources(luigi.postgres.CopyToTable):
    """
    Update ais_sources table with name of CSV processed if ValidMessagesToDatabase has passed
    """

    in_file = luigi.Parameter()
    clean_path = luigi.Parameter()

    null_values = (None,"")
    column_separator = ","

    # host = luigi.Parameter(default='localhost',significant=False)
    # database = luigi.Parameter(default='test_aisdb',significant=False)
    # user = luigi.Parameter(default='postgres',significant=False)
    # password = luigi.Parameter(default='',significant=False)
    # table = luigi.Parameter(default='ais_clean',significant=False)

    host = "localhost"
    database = "pyrate_2015"
    user = "postgres"
    password = ""
    table = "ais_sources"

    def requires(self):
        return ValidMessagesToDatabase(self.in_file, self.clean_path)

    def run(self):
        # Prepare source data to add to ais_sources
        source_data = {'filename':self.in_file,'ext':os.path.splitext(self.in_file)[1],'invalid':0,'clean':0,'dirty':0,'source':0}
        columns = '(' + ','.join([c.lower() for c in source_data.keys()]) + ')'

        connection = self.output().connect()
        cursor = connection.cursor()
        with cursor:
            tuplestr = "(" + ",".join("%({})s".format(i) for i in source_data.keys()) + ")"
            cursor.execute("INSERT INTO " + self.table + " "+ columns + " VALUES "+ tuplestr, source_data)

        # mark as complete
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()
