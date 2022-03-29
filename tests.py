import math
import unittest
# pandas is used to data validation in tests
import pandas as pd
from io import StringIO
import sys
# import the script file
import join


class TestDataJoin(unittest.TestCase):
    def setUp(self):
        # save filepath
        self.filepath1 = "data/data11.csv"
        self.filepath2 = "data/data12.csv"
        # open files with pandas to compare results in the tests
        self.first_test_validator = pd.read_csv(self.filepath1)
        self.second_test_validator = pd.read_csv(self.filepath2)


    """ Check if the generator reads all of the csv file records. """
    def test_generator_record_count(self):
        # create generator
        generator = join.csv_record_generator(self.filepath1)
        num_of_records = 0
        # omit header
        _ = next(generator)
        # count records
        for _ in generator:
            num_of_records += 1
        # read csv with pandas to check if record number is correct
        csv_pandas = pd.read_csv(self.filepath1)
        self.assertEqual(len(csv_pandas), num_of_records, "Record counts are not equal.")


    """ Check if returned joining column indexes and final headers are accurate. """
    def test_data_preparation(self):
        join_column = "day"
        join_type = "inner"
        first_join_col_idx, second_join_col_idx, final_headers = join.prepare_data(self.filepath1, self.filepath2,
                                                                                   join_column, join_type)

        header_columns = final_headers.split(",")

        joined_files = pd.merge(self.first_test_validator, self.second_test_validator, join_type)
        column_after_join_count = len(joined_files.columns)

        print(joined_files.head())

        self.assertEqual(1, first_join_col_idx, "First file join column index is incorrect.")
        self.assertEqual(1, second_join_col_idx, "Second file join column index is incorrect.")
        self.assertEqual(column_after_join_count, len(header_columns), "Header has wrong column number.")


    """ Check if returned joined data has correct record count. """
    def template_test_record_count(self, join_type):
        # redirect stdout
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()

        # join the data
        join.perform_join(self.filepath1, self.filepath2, "day", join_type)

        # get back to the old stdout
        sys.stdout = old_stdout
        # get value of the printed output to string
        stdout_string = mystdout.getvalue()

        # join data with
        joined_files = pd.merge(self.first_test_validator, self.second_test_validator, join_type)
        # manual comparison of the first record can be done
        print(joined_files.loc[0])

        # print caught data
        print(stdout_string)
        # omit blank records and header when counting
        records_count = [x for x in stdout_string.split("\n") if x != ''][1:]
        print(records_count)
        self.assertEqual(len(joined_files), len(records_count), "There are missing records in the joined data.")

    """ Check if returned joined data has correct record count - inner join """
    def test_record_count_inner(self):
        self.template_test_record_count("inner")

    """ Check if returned joined data has correct record count - left join """
    def test_test_record_count_left(self):
        self.template_test_record_count("left")

    """ Check if returned joined data has correct record count - right join """
    def test_test_record_count_right(self):
        self.template_test_record_count("right")


    """ Check if each column has corresponding value. """
    def template_test_column_values_count(self, join_type):
        # redirect stdout
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()

        # join the data
        join.perform_join("data/data11.csv", "data/data12.csv", "day", join_type)

        # get back to the old stdout
        sys.stdout = old_stdout
        # get value of the printed output to string
        stdout_string = mystdout.getvalue()

        # join data with
        joined_files = pd.merge(self.first_test_validator, self.second_test_validator, join_type)

        # print caught data
        print(stdout_string)
        # omit blank records and header when counting
        records = [x for x in stdout_string.split("\n") if x != ''][1:]

        # count of record values
        min_val_count = math.inf
        max_val_count = 0

        for rec in records:
            record_val_count = len(rec.split(","))
            if record_val_count < min_val_count:
                min_val_count = record_val_count
            if record_val_count > max_val_count:
                max_val_count = record_val_count

        # column min and max should be equal so its not compulsory to calculate avg
        col_val_count = (min_val_count + max_val_count) / 2

        # check if each row contains the same number of values
        self.assertTrue(min_val_count == max_val_count, "Number of column values in records are divergent")
        # check if column value for each row occurs
        self.assertEqual(len(joined_files.columns), col_val_count, "There are missing in records")


    """ Check if each column has corresponding value - inner join """
    def test_column_values_count_inner(self):
        self.template_test_column_values_count("inner")


    """ Check if each column has corresponding value - left join """
    def test_column_values_count_left(self):
        self.template_test_column_values_count("left")


    """ Check if each column has corresponding value - right join """
    def test_column_values_count_right(self):
        self.template_test_column_values_count("right")



if __name__ == '__main__':
    unittest.main()
