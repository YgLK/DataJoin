# generators are used to prevent lack of memory to store the data in the machine
def csv_record_generator(filepath):
    for row in open(filepath):
        yield row

def inner_join(filepath1, filepath2, join_column):
    # get data from files with the use of generators
    generator1 = csv_record_generator(filepath1)
    generator2 = csv_record_generator(filepath2)

    # get csv files columns list
    header1 = next(generator1).replace("\n", "")
    header2 = next(generator2).replace("\n", "")
    columns_names_1 = header1.split(",")
    columns_names_2 = header2.split(",")

    # get join column indexes
    join_col_idx1 = columns_names_1.index(join_column)
    join_col_idx2 = columns_names_2.index(join_column)

    # print joined table header, remove duplicated join column
    inner_join_headers = header1 + "," + header2.replace(join_column + ",", "")
    print(inner_join_headers)

    # iterate through rows from each file and perform inner join
    for row in generator1:
        # remove break line from the row
        row = row.replace("\n", "")
        # get list of column values in the actual row from first file
        split_row1 = row.split(",")
        # redefine second generator to go through each row repeatedly
        generator2 = csv_record_generator(filepath2)
        # omit headers
        _ = next(generator2)
        for joined_row in generator2:
            # remove break line from the joined row
            joined_row = joined_row.replace("\n", "")
            # get list of column values in the actual row from second file
            split_row2 = joined_row.split(",")
            # check if corresponding values allows to perform inner join
            if split_row1[join_col_idx1] == split_row2[join_col_idx2]:
                # remove duplicated join column
                row_to_join = joined_row.replace(split_row2[join_col_idx2] + ",", "")
                joined_row = row + "," + row_to_join
                print(joined_row)


if __name__ == "__main__":
    # test with the sample data
    inner_join("data/borrower.csv", "data/loan.csv", "LOAN_NO")

