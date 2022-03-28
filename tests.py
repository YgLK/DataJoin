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

    """
    Check if the generator reads all of the csv file records.
    """
    def test_generator_record_count(self): # DONE
        # create generator
        generator = join.csv_record_generator(self.filepath1)
        num_of_records = 0
        # omit header
        _ = next(generator)
        # count records
        for record in generator:
            num_of_records += 1
        # read csv with pandas to check if record number is correct
        csv_pandas = pd.read_csv(self.filepath1)
        # assert equality
        self.assertEqual(len(csv_pandas), num_of_records, "Record counts are not equal.")


    """
    Check if returned joining column indexes and final headers are accurate.
    """
    def test_data_preparation(self):    # DONE
        join_column = "day"
        join_type = "inner"
        first_join_col_idx, second_join_col_idx, final_headers = join.prepare_data(self.filepath1, self.filepath2,
                                                                                   join_column, join_type)

        header_columns = final_headers.split(",")

        joined_files = pd.merge(self.first_test_validator, self.second_test_validator, join_type)
        column_after_join_count = len(joined_files.columns)

        print(joined_files.head())

        self.assertEqual(1, first_join_col_idx, "First file join column index is incorrect")
        self.assertEqual(1, second_join_col_idx, "Second file join column index is incorrect")
        # DATE column name in the date12.csv had to be changed to DATE2
        # because caused overlapping in the pandas merge (date from the second file didn't exist in the joined_files)
        self.assertEqual(column_after_join_count, len(header_columns), "Header has wrong column number.")



    def template_test_column_values(self, join_type): # TODO
        # redirect stdout to string
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()

        # join the data
        join.perform_join("data/borrower.csv", "data/loan.csv", "LOAN_NO", join_type)
        # join data with
        joined_files = pd.merge(self.first_test_validator, self.second_test_validator, join_type)
        # manual comparison of the first record can be done
        print(joined_files.loc[0])

        sys.stdout = old_stdout
        # print caught data
        stdout_string = mystdout.getvalue()
        print(stdout_string)


    """
    Check if all rows has the same length/column values - inner join
    """
    def test_column_values_count_inner(self):
        self.template_test_column_values("inner")

    """
    Check if all rows has the same length/column values - left join
    """
    def test_column_values_count_left(self):
        self.template_test_column_values("left")

    """
    Check if all rows has the same length/column values - right join
    """
    def test_column_values_count_right(self):
        self.template_test_column_values("right")


    """
    Check if all rows has the same length/column values - right join
    """
    # def test_column_values_count_right(self):
    #     output = ""
        # # redirect stdout to string
        # with io.StringIO() as buf, redirect_stdout(buf):
        #     join.perform_join(self.filepath1, self.filepath2, "day", "inner")
        #     output = buf.getvalue()
        # print(output)


    """
    Check if each join type has correct record count
    """
    # def test_join_record_count(self):
    #     return "TODO"

if __name__ == '__main__':
    unittest.main()
