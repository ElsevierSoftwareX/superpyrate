"""
"""
import csv
import os
import sys
from pyrate.algorithms.aisparser import readcsv, parse_raw_row, \
                                        AIS_CSV_COLUMNS, \
                                        validate_row

def produce_valid_csv_file(input_file, output_file):
    """
    Arguments
    ---------
    input_file :
        File path to a large CSV file of AIS data
    output_file :
        File path for a CSV file containing validated and cleaned data
    """

    # if os.path.isfile(input_file) == False:
    #     raise RuntimeError("Input file does not exist")

    # Read input_file
    iterator = readcsv(input_file)
    # Do validation and write a new file of valid messages
    writer = csv.DictWriter(output_file, dialect="excel", fieldnames=AIS_CSV_COLUMNS)
    writer.writeheader()

    # parse and iterate lines from the current file
    for row in iterator:
        converted_row = {}
        try:
            # parse raw data
            converted_row = parse_raw_row(row)
        except ValueError as e:
            # invalid data in row. Write it to error log
            if not 'raw' in row:
                row['raw'] = [row[c] for c in AIS_CSV_COLUMNS]
            continue
        except KeyError:
            # missing data in row.
            if not 'raw' in row:
                row['raw'] = [row[c] for c in AIS_CSV_COLUMNS]
            continue

        # validate parsed row
        try:
            validated_row = validate_row(converted_row)
            writer.writerow(validated_row)
        except ValueError:
            pass


if __name__ == "__main__":
    input_file = sys.argv[0]
    output_file = sys.argv[1]
    produce_valid_csv_file(input_file, output_file)
