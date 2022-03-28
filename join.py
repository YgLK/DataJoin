#!/usr/bin/env python3
import sys


# generators are used to prevent lack of memory to store the data in the local machine
def csv_record_generator(filepath):
    with open(filepath) as csv_file:
        for record in csv_file:
            yield record


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
    # get join column indexes
    join_col_idx1 = columns_names_1.index(join_col)
    join_col_idx2 = columns_names_2.index(join_col)

    return join_col_idx1, join_col_idx2, final_headers


def inner_join(first_path, second_path, first_join_col_idx, second_join_col_idx):
    first_gen = csv_record_generator(first_path)
    # omit headers
    _ = next(first_gen)
    # iterate through rows from each file and perform inner join
    for first_row in first_gen:
        # remove break line from the row
        first_row = first_row.replace("\n", "")
        # get list of column values in the actual row from first file
        first_row_split = first_row.split(",")
        # redefine second generator to go through each row repeatedly
        second_gen = csv_record_generator(second_path)
        # omit headers
        _ = next(second_gen)
        for second_row in second_gen:
            # remove break line from the joined row
            second_row = second_row.replace("\n", "")
            # get list of column values in the actual row from second file
            second_row_split = second_row.split(",")
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
        # remove break line from the row
        first_row = first_row.replace("\n", "")
        # get list of column values in the actual row from first file
        first_row_split = first_row.split(",")
        # redefine second generator to go through each row repeatedly
        second_gen = csv_record_generator(second_filepath)
        # omit headers
        _ = next(second_gen).split(",")
        # flag to check if left row found corresponding right row
        is_joined = False
        # count of missing values to fill when the left join doesnt find corresponding right row
        sec_row_Len = len(_) - 1
        for second_row in second_gen:
            # remove break line from the joined row
            second_row = second_row.replace("\n", "")
            # get list of column values in the actual row from second file
            second_row_split = second_row.split(",")
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


def perform_join(filepath1, filepath2, join_col, join):
    # data preparation
    first_join_col_idx, second_join_col_idx, final_headers = prepare_data(filepath1, filepath2, join_col, join)

    # print joined table header with removed duplicated join column name
    print(final_headers)

    # perform the join of two csv files
    if join == "inner":
        inner_join(filepath1, filepath2, first_join_col_idx, second_join_col_idx)
    elif join == "left":
        left_join(filepath1, filepath2, first_join_col_idx, second_join_col_idx)
    # right_join can be performed by using left_join method with the file paths and
    # join columns indexes swapped with each other
    elif join == "right":
        left_join(filepath2, filepath1, second_join_col_idx, first_join_col_idx)


if __name__ == "__main__":
    # read arguments
    first_filepath, second_filepath = sys.argv[1], sys.argv[2]
    join_column = sys.argv[3]
    # set default join type as inner
    join_type = "inner"
    if len(sys.argv) == 5:
        join_type = sys.argv[4]

    perform_join(first_filepath, second_filepath, join_column, join_type)


    # test with the sample data
    # perform_join("data/loan.csv", "data/borrower.csv", "LOAN_NO", "inner")
    # test with the different data
    # perform_join("data/data11.csv", "data/data12.csv", "date", "left")

