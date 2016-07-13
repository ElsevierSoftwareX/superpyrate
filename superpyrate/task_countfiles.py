"""Holds the luigi tasks which count the number of rows in the files

Records the number of clean and dirty rows in the AIS data,
writing these stats to the database and finally producing a report of the
statistics

1. Count the number of rows in the raw csv files (in ``files/unzipped/<archive>``)
2. Count the number of rows int the clean csv files (in ``files/cleancsv/``)
3. Write the clean rows in the clean column of ais_sources
4. Write the dirty (raw - clean) rows into the dirty column of ais_sources

"""
import luigi
from luigi.util import requires
from luigi.contrib.external_program import ExternalProgramTask
from luigi.postgres import CopyToTable, PostgresQuery
from superpyrate.pipeline import get_environment_variable, ProcessZipArchives, \
                                 GetZipArchive, get_working_folder, \
                                 RunQueryOnTable, GetCsvFile
from plumbum.cmd import wc
from glob import glob
import os
import logging
LOGGER = logging.getLogger(__name__)

logging.basicConfig(filename='reporting.log',
                    level=logging.DEBUG,
                    filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

@requires(ProcessZipArchives)
class ProduceStatisticsReport(PostgresQuery):
    """Produces a report of the data statistics
    """
    host = get_environment_variable('DBHOSTNAME')
    database = get_environment_variable('DBNAME')
    user = get_environment_variable('DBUSER')
    password = get_environment_variable('DBUSERPASS')
    query = "SELECT filename, clean, dirty, "\
            "(clean / (CASE dirty WHEN 0 THEN NULL ELSE dirty END) * 100) as coverage " \
            "FROM ais_sources;"
    table = 'ais_sources'
    update_id = 'stats_report'

    def run(self):
        """Produce the report and write to a file
        """
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query

        LOGGER.info('Executing query from task: {name}'.format(name=self.__class__))
        cursor.execute(sql)
        working_folder = get_working_folder()
        path = os.path.join(working_folder, 'files', 'data_statistics.csv')
        with open(path, 'w') as report_file:
            for row in cursor.fetchall():
                report_file.writeline(row)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()


@requires(ProcessZipArchives)
class GetCountsForAllFiles(luigi.Task):
    """Counts the rows in all clean (validated) and raw files
    """
    def run(self):
        """
        """
        working_folder = get_working_folder()
        paths_to_count = [os.path.join(working_folder, 'files', 'cleancsv')]
        with self.input().open('r') as list_of_archives:

            for archive in list_of_archives:
                filename = os.path.basename(archive)
                #Remove extension
                name, ext = os.path.splitext(filename)
                # count lines of each file in each unzipped archive stored in
                # LUIGIWORK/files/unzipped/<name>
                path = os.path.join(working_folder, 'files', 'unzipped', name)
                LOGGER.debug("Input path: {}".format(path))
                if str(ext).strip() == '.zip':
                    paths_to_count.append(path)
        yield [CountLines(countable_path) for countable_path in paths_to_count]

        with self.output().open('w') as outfile:
            outfile.write("Finished task")

    def output(self):
        rootdir = get_working_folder()
        output_folder = os.path.join(rootdir,'tmp', 'countraw', 'got_all_counts.txt')
        return luigi.file.LocalTarget(output_folder)


@requires(GetZipArchive)
class CountLines(luigi.Task):
    """Counts the number of lines for all the csvfiles in a folder

    Writes all the counts and filenames to a delimited file with the name of the
    folder

    Arguments
    =========
    folder_name : str
        The absolute path of the csv file

    """

    def run(self):
        """Runs the bash program `wc` to count the number of lines

        """
        input_names = glob(self.input().fn + '/*')
        output_name = self.output().fn
        LOGGER.debug('Counting lines in {} and saving to {}'.format(input_names, output_name))
        (wc['-l', input_names] > output_name)()

    def output(self):
        """Outputs the files into a folder of the same name as the zip file

        The files are placed in a subdirectory of ``LUIGIWORK`` called ``tmp/countraw``
        """
        out_folder_name = os.path.basename(self.input().fn)
        rootdir = get_working_folder()
        output_folder = os.path.join(rootdir,'tmp', 'countraw', out_folder_name + ".csv")
        return luigi.file.LocalTarget(output_folder)



class DoIt(luigi.Task):
    """
    """
    folder_of_zips = luigi.Parameter(significant=True)
    with_db = luigi.BoolParameter(significant=False)

    def requires(self):
        working_folder = get_working_folder()
        clean_path = os.path.join(working_folder, 'tmp', 'countraw', 'cleancsv.csv')
        return [GetCsvFile(clean_path), GetCountsForAllFiles(self.folder_of_zips, self.with_db)]

    def run(self):
        clean_counts = self.input()[0]
        raw_counts = self.input()[1]
        with clean_counts.open('r') as clean_counts_file:
            for row in clean_counts_file:
                count, filename = row.split(" ")



class WriteRawCountsToSourceTable(luigi.Task):
    """
    """
    filename = luigi.Parameter(significant=True)
    clean_count = luigi.IntParameter(significant=False)
    dirty_count = luigi.IntParameter(significant=False)

    def run(self):
        query = "UPDATE ais_sources "\
                "SET clean = {}, dirty = {} " \
                "WHERE filename = {};".format(clean_count,
                                              dirty_count,
                                              filename)
        table = 'ais_sources'

        return RunQueryOnTable(query, table)

    def output(self):
        pass
