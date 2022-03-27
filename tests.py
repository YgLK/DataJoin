import unittest
# pandas used to check algorithm correctness
import pandas as pd
# import the script file
import main


class TestDataJoin(unittest.TestCase):
    """
    Check if the generator reads all of the csv file records.
    """
    def test_generator_record_count(self):
        filepath = "data/data11.csv"
        # create generator
        generator = main.csv_record_generator(filepath)
        num_of_records = 0
        # omit headers
        _ = next(generator)
        # count records
        for record in generator:
            num_of_records += 1
        # read csv with pandas to check if record number is correct
        csv_pandas = pd.read_csv(filepath)
        # assert equality
        self.assertEqual(len(csv_pandas), num_of_records, "Record counts are not equal.")

    """
    Check if returned joining column indexes and final headers are accurate.
    """
    def test_data_preparation(self):
        filepath1 = "data/data11.csv"
        filepath2 = "data/data12.csv"
        join_column = "day"
        join_type = "inner"
        first_join_col_idx, second_join_col_idx, final_headers = main.prepare_data(filepath1, filepath2, join_column,
                                                                                   join_type)
        # open files with pandas to compare results
        first_test_validator = pd.read_csv(filepath1)
        second_test_validator = pd.read_csv(filepath2)

        self.assertEqual(first_test_validator.columns., "First file join column index is incorrect")
        self.assertEqual(, num_of_records, "Second file join column index is incorrect")
        self.assertEqual(, num_of_records, "First file join column index is incorrect")
        self.assertEqual(, num_of_records, "Second file join column index is incorrect")


    """
    Returns length of info axis, but here we use the index.
    """

    def test_sum_tuple(self):
        self.assertEqual(sum((1, 2, 2)), 6, "Should be 6")


if __name__ == '__main__':
    unittest.main()
