import pandas as pd

def validation_process(data, table_name):
    print("============ Start Pipeline Validation ============")
    print("")

    validation = []

    # check data shape
    n_rows = data.shape[0]
    n_cols = data.shape[1]

    print(f"{table_name} has {n_rows} rows and {n_cols} columns")
    print("")

    validation.append(f"{table_name} has {n_rows} rows and {n_cols} columns")

    GET_COLS = data.columns

    # check data types for each column
    for col in GET_COLS:
        print(f"Column {col} has data type {data[col].dtypes}")
        validation.append(f"Column {col} has data type {data[col].dtypes}")

    print("")

    # check missing values for each column
    for col in GET_COLS:
    # calculate missing values in percentage
        get_missing_values = (data[col].isnull().sum() * 100) / len(data)
        print(f"Columns {col} has percentages missing values: {get_missing_values} %")
        validation.append(f"Columns {col} has percentages missing values: {get_missing_values} %")

    print("")
    print("============ End Pipeline Validation ============")
    return validation