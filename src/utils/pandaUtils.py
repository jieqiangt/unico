import pandas as pd


def standardize_col_names(df):

    new_cols = []
    for col in df.columns:
        col = col.lower()
        col = col.replace(".", "")
        col = col.replace("/", "")
        col = col.replace("(", "")
        col = col.replace(")", "")
        col = col.replace(" ", "_")
        new_cols.append(col)

    df.columns = new_cols

    return df


def convert_dt_cols(df, cols, dt_format="%Y%m%d"):

    for col in cols:
        df[col] = pd.to_datetime(df[col], format=dt_format)

    return df


def convert_numeric_cols(df, cols):

    for col in cols:
        df[col] = df[col].str.replace(",", "")
        df[col] = pd.to_numeric(df[col])

    return df


def merge_all_df(dfs, merge_keys):

    output_df = dfs.pop(0)

    for df in dfs:
        output_df = output_df.merge(
            df, on=merge_keys, how='left')

    return output_df


def filter_df_by_date_range(df, date_col, start_date, end_date, inclusive=True):

    cond = ((df[date_col] >= start_date) & (df[date_col] <= end_date))

    if not inclusive:
        cond = ((df[date_col] > start_date) & (df[date_col] < end_date))

    return df[cond].reset_index(drop=True)


def create_csv_with_cols(file_path, input_file_name, output_file_name, cols):

    df = pd.read_csv(f"{file_path}/{input_file_name}",
                     encoding='utf-8',  header=0, names=cols)
    df.to_csv(f"{file_path}/{output_file_name}", index=False, encoding='utf-8')
    del df


def get_date_cols(df):

    return list(filter(lambda col_name: 'date' in col_name, list(df.columns)))
