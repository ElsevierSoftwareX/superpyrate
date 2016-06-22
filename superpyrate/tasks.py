"""
"""
import csv
import sys
from pyrate.algorithms.aisparser import parse_raw_row, \
                                        AIS_CSV_COLUMNS, \
                                        validate_row
# from exactVerify.ais_import.algorithms.exact_verifyparser import readcsv
import logging
from fuzzywuzzy import process as fuzz_proc

LOGGER = logging.getLogger('luigi-interface')

FORCED_COL_MAP = {'MMSI': 'MMSI',
                  'Time': 'Time',
                  'Message_ID': 'Message_ID',
                  'Navigational_status': 'Navigational_status',
                  'SOG': 'SOG',
                  'Longitude': 'Longitude',
                  'Latitude': 'Latitude',
                  'COG': 'COG',
                  'Heading': 'Heading',
                  'IMO': 'IMO',
                  'Draught': 'Draught',
                  'Destination': 'Destination',
                  'Vessel_Name': 'Vessel_Name',
                  'ETA_month': 'ETA_month',
                  'ETA_day': 'ETA_day',
                  'ETA_hour': 'ETA_hour',
                  'ETA_minute': 'ETA_minute'}


def learn_columns(read_cols, required_cols, csv_or_xml='csv'):
    if csv_or_xml == 'csv':
        matched_cols = {}
        for col in required_cols:
            matched_cols[col] = fuzz_proc.extractOne(col, read_cols)
        return matched_cols
    else:
        return read_cols


def produce_valid_csv_file(inputf, outputf):
    """
    Arguments
    ---------
    input_file :
        File path to a large CSV file of AIS data
    output_file :
        File path for a CSV file containing validated and cleaned data
    """
    LOGGER.info("Processing {}".format(inputf))
    # Read input_file
    # try:
    columns = AIS_CSV_COLUMNS

    with open(inputf, 'rU') as input_file:
        # Do validation and write a new file of valid messages
        with open(outputf, 'w') as output_file:
            writer = csv.DictWriter(output_file,
                                    dialect="excel",
                                    fieldnames=columns)
            writer.writeheader()

            # parse and iterate lines from the current file
            LOGGER.debug("Building the reader")
            reader = unfussy_reader(readcsv(input_file,
                                            forced_col_map=FORCED_COL_MAP,
                                            columns=columns))
            LOGGER.debug("Iterating over the reader")
            for row in reader:
                if len(row) > 0:
                    converted_row = {}
                    try:
                        converted_row = parse_raw_row(row)
                    except ValueError as e:
                        # invalid data in row. Write it to error log
                        LOGGER.error("Invalid data in row: {}".format(e))
                        continue
                    except KeyError as e:
                        LOGGER.error("Missing data in row: {}".format(e))
                        continue
                    else:
                        # validate parsed row
                        validated_row = validate_row(converted_row)
                        try:
                            LOGGER.debug("Attempting writing validated data to file.")
                            writer.writerow(validated_row)
                        except ValueError as ve:
                            LOGGER.error("Error in writing validated row to csvfile: {}".format(ve))
                            continue
                else:
                    LOGGER.info("Illegal row, so not writing to file.")

def unfussy_reader(csv_reader):
    while True:
        try:
            yield next(csv_reader)
        # Catch csv field size limit exceeded error
        except csv.Error as ce:
            LOGGER.error('CSV Error: {} on line {}'.format(ce, csv_reader.line_num))
            yield {}
            continue
        # catch 'ascii' decode error
        except UnicodeDecodeError as ude:
            LOGGER.error('CSV Error: {} on line {}'.format(ude, csv_reader.line_num))
            yield {}
            continue

def readcsv(fp, forced_col_map=None, columns=None):
    """ Yields a dctionary of the subset of columns required

    Reads each line in CSV file, checks if all columns are available,
    and returns a dictionary of the subset of columns required
    (as per AIS_CSV_COLUMNS).

    If row is invalid (too few columns),
    returns an empty dictionary.

    Arguments
    ---------
    fp : TextIOWrapper
        An open TextIOWrapper (returned by `open()`)
    forced_col_map : dict
        A dictionary mapping the keys defined in columns to
        columns with different names
    columns : dict, default=AIS_CSV_COLUMNS
        A dictionary of columns

    Yields
    ------
    rowsubset : dict
        A dictionary of the subset of columns as per `columns`

    """
    max_int = sys.maxsize
    decrement = True
    while decrement:
        # decrease the max_int value by factor 10
        # as long as the OverflowError occurs.
        decrement = False
        try:
            csv.field_size_limit(max_int)
        except OverflowError:
            max_int = int(max_int / 10)
            decrement = True

    # first line is column headers. Use to extract indices of columns
    # we are extracting
    cols = fp.readline().strip('\r\n').split(',')
    LOGGER.info("There are {} columns in {}".format(len(cols), fp.name))
    indices = {}
    auto_col_map = learn_columns(cols, columns)
    # LOGGER.debug("{}".format(auto_col_map))
    used_map = {}

    for col in columns:
        if (len(forced_col_map) > 0) & (col in forced_col_map.keys()):
            try:
                indices[col] = cols.index(forced_col_map[col])
                used_map.update({col: forced_col_map[col]})
            except Exception as e0:
                LOGGER.warning("{}, {}".format(forced_col_map, col))
                LOGGER.warning(cols)
                LOGGER.warning("Error mapping columns: {}".format(repr(e0)))
                if auto_col_map[col][1] >= 95:
                    indices[col] = cols.index(auto_col_map[col][0])
                    used_map.update({col: auto_col_map[col][0]})
                else:
                    raise RuntimeError("Something wrong with provided column map. \
                                        Double check key-value pairs. \
                                        Note that names are case sensitive. {0}.".format(e0))
        else:
            try:
                # LOGGER.debug(cols)
                indices[col] = cols.index(col)
                used_map.update({col: col})
            except Exception as e1:
                LOGGER.warning("Error mapping columns: {}".format(repr(e1)))
                if auto_col_map[col][1] >= 95:
                    indices[col] = cols.index(auto_col_map[col][0])
                    used_map.update({col: auto_col_map[col][0]})
                else:
                    auto_col_map_msg = '{' + ', '.join("'{!s}':{!r}".format(
                        key, val[0]) for (key, val) in auto_col_map.items()) + '}'
                    msg = "Possibly ambiguous column mapping or column missing "\
                        "in CSV header. Try specifying an exact map as a "\
                        "dictionary to exact_verify's 'run' command. "\
                        "Suggested column mapping (required:read): {}".format(
                            auto_col_map_msg)
                    raise RuntimeError(
                        "Missing columns in file header: {0}. {1}.".format(e1, msg))

    # Show message showing column map used for current file
    LOGGER.debug(
        "Using the following column mapping (required:read): {}".
        format(used_map))

    unfussy = unfussy_reader(csv.reader(fp, delimiter=',', quotechar='"'))

    for row in unfussy:
        # LOGGER.info("New row")
        rowsubset = {}
        # only try to process row if all columns are available
        # changed from >= to ==
        if len(row) == len(cols):
            for col in columns:
                rowsubset[col] = row[indices[col]]  # raw column data
            LOGGER.debug("Legal row found: {}".format(rowsubset))
            yield rowsubset
        else:
            LOGGER.info("""Expected column length doesn't match row in file: {}.
                        Row is {}, column is {}""".format(fp.name, len(row), len(cols)))
            yield rowsubset

if __name__ == "__main__":
    an_input_file = sys.argv[0]
    an_output_file = sys.argv[1]
    produce_valid_csv_file(an_input_file, an_output_file)
