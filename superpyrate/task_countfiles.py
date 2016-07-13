"""Holds the luigi tasks which count the number of rows in the files

Records the number of clean and dirty rows in the AIS data,
writing these stats to the database and finally producing a report of the
statistics
"""
import luigi
from luigi.util import requires
from luigi.contrib.external_program import ExternalProgramTask
from luigi.postgres import CopyToTable, PostgresQuery
from superpyrate.pipeline import get_environment_variable, ProcessZipArchives, \
                                 GetZipArchive, get_working_folder
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
    query = "SELECT filename, clean, dirty, (clean / dirty * 100) as coverage " \
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

        with open('files/data_statistics.csv', 'w') as report_file:
            for row in cursor.fetchall():
                report_file.writeline(row)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()


@requires(ProcessZipArchives)
class GetCountsForAllFiles(luigi.Task):
    """
    """
    def run(self):
        """
        """
        paths_to_count = []

        with self.input().open('r') as list_of_archives:
            working_folder = get_working_folder()
            for archive in list_of_archives:
                # count lines of each file in each unzipped archive stored in
                # LUIGIWORK/files/unzipped/<name>
                path = os.path.join(working_folder, 'files', 'unzipped', archive)
                paths_to_count.append(path)
        yield [CountLines(path) for path in paths_to_count]

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
        (wc['-l', input_names] > output_name)()

    def output(self):
        """Outputs the files into a folder of the same name as the zip file

        The files are placed in a subdirectory of ``LUIGIWORK`` called ``tmp/countraw``
        """
        out_folder_name = os.path.basename(self.input().fn)
        rootdir = get_working_folder()
        output_folder = os.path.join(rootdir,'tmp', 'countraw', out_folder_name + ".csv")
        return luigi.file.LocalTarget(output_folder)
