from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from jinjasql import JinjaSql
import pandas as pd


def create_mssql_engine(driver, server, db_name, db_user, db_pw):

    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={
        db_name};UID={db_user};PWD={db_pw};TrustServerCertificate=yes"
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    return engine


def create_mysql_engine(host, db_name, db_user, db_pw, ssl_ca=None):

    if ssl_ca:
        connection_url = f"mysql+pymysql://{db_user}:{db_pw}@{host}/{db_name}?ssl_ca={ssl_ca}"
    else:
        connection_url = f"mysql+pymysql://{db_user}:{db_pw}@{host}/{db_name}"

    engine = create_engine(
        connection_url, insertmanyvalues_page_size=3000, use_insertmanyvalues=True)

    return engine


def get_query_from_sql_file(file_name, params=None):
    jinja = JinjaSql(param_style='pyformat')

    with open(file_name) as my_file:
        template = my_file.read()

    if params:
        query, bind_params = jinja.prepare_query(template, params)
        return text(query % bind_params)

    return text(template)


def execute_in_mysql(conn, file_path, params=None):

    # do a connection check to check if it is a mysql connection
    query = get_query_from_sql_file(file_path, params)
    print(query)
    data = conn.execute(query)

    return data


def get_data_from_query(conn, file_path, params=None):

    if params:
        get_data_query = get_query_from_sql_file(file_path, params)
    else:
        get_data_query = get_query_from_sql_file(file_path)

    data = pd.read_sql(get_data_query, conn)

    return data


def drop_table(conn, table):

    drop_query = get_query_from_sql_file(
        f'./sql/mysql/delete/drop_table.sql', params={"table": table})
    conn.execute(drop_query)
