#!/usr/bin/env python3
import sys
import os


# generators are used to prevent lack of memory to store the data in the local machine
def csv_record_generator(filepath):
    # check if file path points to CSV file
    if not filepath.endswith(".csv"):
        raise ValueError("File must have .csv extension")
    # check if file exists
    elif not os.path.isfile(filepath):
        raise FileNotFoundError("File doesn't exist, check entered filepath")
    # read data from file
    with open(filepath) as csv_file:
        for record in csv_file:
            yield record


# prepare data for joining
def prepare_data(path1, path2, join_col, join_type):
    # get data from files with the use of generators
    gen1 = csv_record_generator(path1)
    gen2 = csv_record_generator(path2)

    # get column names lists of each csv file
    header1 = next(gen1).replace("\n", "").upper()
    header2 = next(gen2).replace("\n", "").upper()
    join_col = join_col.upper()
    columns_names_1 = header1.split(",")
    columns_names_2 = header2.split(",")

    # get joined table header, remove duplicated join column name,
    final_headers = ""
    if join_type in ("inner", "left"):
        # second use of the replace method in case when join_col is the last column in the row
        final_headers = header1 + "," + header2.replace(join_col + ",", "").replace("," + join_col, "")
    elif join_type == "right":
        final_headers = header2.replace(join_col + ",", "").replace("," + join_col, "") + "," + header1
    else:
        raise ValueError("Incorrect join type. Possible join types: inner, left, right")

    # get join column indexes
    try:
        join_col_idx1 = columns_names_1.index(join_col)
        join_col_idx2 = columns_names_2.index(join_col)
    except ValueError:
        raise ValueError("Column to be joined on must appear in both CSV files")

    return join_col_idx1, join_col_idx2, final_headers


def split_record(first_row):
    # remove break line from the row
    first_row = first_row.replace("\n", "")
    # get list of column values in the actual row from first file
    first_row_split = first_row.split(",")
    return first_row, first_row_split


def inner_join(first_path, second_path, first_join_col_idx, second_join_col_idx):
    first_gen = csv_record_generator(first_path)
    # omit headers
    _ = next(first_gen)
    # iterate through rows from each file and perform inner join
    for first_row in first_gen:
        first_row, first_row_split = split_record(first_row)
        # redefine second generator to go through each row repeatedly
        second_gen = csv_record_generator(second_path)
        # omit headers
        _ = next(second_gen)
        for second_row in second_gen:
            second_row, second_row_split = split_record(second_row)
            # check if corresponding values allows to perform inner join
            if first_row_split[first_join_col_idx] == second_row_split[second_join_col_idx]:
                # remove element from split row to avoid repeating columns
                second_row_split.pop(second_join_col_idx)
                row_to_join = ",".join(second_row_split)
                # remove duplicated join column
                joined_row = first_row + "," + row_to_join
                print(joined_row)


def left_join(first_filepath, second_filepath, first_join_col_idx, second_join_col_idx):
    first_gen = csv_record_generator(first_filepath)
    # omit headers
    _ = next(first_gen)
    # iterate through rows from each file and perform inner join
    for first_row in first_gen:
        first_row, first_row_split = split_record(first_row)
        # redefine second generator to go through each row repeatedly
        second_gen = csv_record_generator(second_filepath)
        # omit headers
        _ = next(second_gen).split(",")
        # flag to check if left row found corresponding right row
        is_joined = False
        # count of missing values to fill when the left join doesnt find corresponding right row
        sec_row_Len = len(_) - 1
        for second_row in second_gen:
            second_row, second_row_split = split_record(second_row)
            # check if corresponding values allows to perform left join
            if first_row_split[first_join_col_idx] == second_row_split[second_join_col_idx]:
                # remove element from split row to avoid repeating columns
                second_row_split.pop(second_join_col_idx)
                row_to_join = ",".join(second_row_split)
                # remove duplicated join column
                joined_row = first_row + "," + row_to_join
                print(joined_row)
                is_joined = True
        if not is_joined:
            # fill missing value with NaN
            joined_row = first_row + "," + "NaN," * sec_row_Len
            # print record with omitted last comma character
            print(joined_row[:-1])


# merge data with the specified join type
def perform_join(first_filepath, second_filepath, join_col, join):
    # data preparation
    first_join_col_idx, second_join_col_idx, final_headers = prepare_data(first_filepath, second_filepath, join_col,
                                                                          join)

    # print joined table header with removed duplicated join column name
    print(final_headers)

    # perform the join of two csv files
    if join == "inner":
        inner_join(first_filepath, second_filepath, first_join_col_idx, second_join_col_idx)
    elif join == "left":
        left_join(first_filepath, second_filepath, first_join_col_idx, second_join_col_idx)
    # right_join can be performed by using left_join method with the file paths and
    # join columns indexes swapped with each other
    elif join == "right":
        left_join(second_filepath, first_filepath, second_join_col_idx, first_join_col_idx)


if __name__ == "__main__":
    # read arguments
    first_csv_filepath, second_csv_filepath = sys.argv[1], sys.argv[2]
    if len(sys.argv) < 4:
        raise ValueError("Column name to join on is missing")
    join_column = sys.argv[3]
    # set default join type as inner
    join = "inner"
    if len(sys.argv) == 5:
        join = sys.argv[4]

    perform_join(first_csv_filepath, second_csv_filepath, join_column, join)
