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
            for countable_path in paths_to_count:
                outfile.write("{}\n".format(countable_path))

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
        return GetCountsForAllFiles(self.folder_of_zips, self.with_db)

    def run(self):
        working_folder = get_working_folder()
        count_folder = os.path.join(working_folder, 'tmp', 'countraw')
        clean_path = os.path.join(count_folder, 'cleancsv.csv')

        countfiles = os.listdir(count_folder)
        LOGGER.debug("Files in tmp/countraw: {}".format(countfiles))
        filtered_countfiles = [a for a in countfiles if (a != 'cleancsv.csv' and a.endswith('.csv'))]
        LOGGER.debug("Files in tmp/countraw after filtering: {}".format(filtered_countfiles))
        if len(filtered_countfiles) == 0:
            raise RuntimeError("No counted files available")
            LOGGER.error("No counted files available")

        clean_results = {}
        raw_results = {}
        counts = []
        with open(clean_path, 'r') as clean_counts_file:
            for row in clean_counts_file:
                LOGGER.debug(row.strip().split(" "))
                count, filename = row.strip().split(" ")
                just_filename = os.path.basename(filename)
                clean_results[just_filename] = int(count)

        for filename in filtered_countfiles:
            results_file = os.path.join(count_folder, filename)
            with open(results_file, 'r') as open_results_file:
                for row in open_results_file:
                    LOGGER.debug(row.strip().split(" "))
                    count, filename = row.strip().split(" ")
                    just_filename = os.path.basename(filename)
                    raw_results[just_filename] = int(count)
        _ = raw_results.pop('total')
        _ = clean_results.pop('total')
        LOGGER.debug("Keys: {}; {}".format(raw_results.keys(), clean_results.keys()))
        for filename in raw_results.keys():
            clean = clean_results[filename]
            dirty = raw_results[filename] - clean
            # filepath = os.path.join(working_folder, 'files', 'unzipped', filename)
            counts.append((filename, clean, dirty))
        LOGGER.debug("Counts of file {}".format(counts))

        queries = [("UPDATE ais_sources "\
                   "SET clean = {}, dirty = {} " \
                   "WHERE filename = '{}';".format(clean_count,
                                                   dirty_count,
                                                   filename), filename)
                   for (filename, clean_count, dirty_count) in counts ]
        table = 'ais_sources'
        yield [RunQueryOnTable(query, table, id) for query, id in queries]

        with self.output().open('w') as outfile:
            outfile.write("Done")

    def output(self):
        working_folder = get_working_folder()
        path = os.path.join(working_folder, 'tmp', 'database', 'reports.txt')
        return luigi.file.LocalTarget(path)



@requires(DoIt)
class ProduceStatisticsReport(PostgresQuery):
    """Produces a report of the data statistics
    """
    host = get_environment_variable('DBHOSTNAME')
    database = get_environment_variable('DBNAME')
    user = get_environment_variable('DBUSER')
    password = get_environment_variable('DBUSERPASS')
    query = "SELECT filename, clean, dirty, round(1.0*dirty/(clean+dirty), 2) " \
            "AS coverage FROM ais_sources ORDER BY filename ASC;"
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
            report_file.write("{} {} {} {}".format('filename', 'clean', 'dirty', 'coverage'))
            for filename, clean, dirty, coverage in cursor.fetchall():
                report_file.write("{} {} {} {}".format(filename, clean, dirty, coverage))

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()
