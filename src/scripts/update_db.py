
from utils.dbUtils import create_mssql_engine, create_mysql_engine, execute_in_mysql, get_data_from_query
from utils.dataProcessing import process_ft_cashflow_monthly_ts, process_ft_cashflow_monthly_by_type_ts, process_ft_suppliers_monthly_pv_ts, process_ft_sales_orders_alerts, process_ft_pdt_summary, process_ft_purchases_alerts, process_ft_sales_agent_performance_ts, process_ft_recent_sales, process_ft_recent_purchases, process_ft_pdt_monthly_summary_ts, process_ft_recent_ar_invoices, process_ft_recent_ap_invoices
from utils.logging import record_data_refresh_log
import os
from dotenv import load_dotenv
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from datetime import datetime
load_dotenv()

MSSQL_CREDS = {"driver": os.getenv('MSSQL_DRIVER'),
               "server": os.getenv('MSSQL_SERVER'),
               "db_name": os.getenv('MSSQL_DB_NAME'),
               "db_user": os.getenv('MSSQL_DB_USER'),
               "db_pw": os.getenv('MSSQL_DB_PW')}

MYSQL_CREDS = {"host": os.getenv('MYSQL_HOST'),
               "db_name": os.getenv('MYSQL_DB_NAME'),
               "db_user": os.getenv('MYSQL_DB_USER'),
               "db_pw": os.getenv('MYSQL_DB_PW'),
               "ssl_ca": os.getenv('MYSQL_SSL_CA'),
               "ssl_cert": os.getenv('MYSQL_SSL_CERT')}

END_DATE = date.today()
END_DATE_STR = END_DATE.strftime("%Y-%m-%d")


def update_ft_cashflow_monthly_ts():

    table = 'ft_cashflow_monthly_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        outgoing = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_outgoing.sql', params)
        incoming = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_incoming.sql', params)

    cashflow_monthly_ts = process_ft_cashflow_monthly_ts(outgoing, incoming)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        cashflow_monthly_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)


def update_ft_cashflow_monthly_by_type_ts():

    table = 'ft_cashflow_monthly_by_type_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        outgoing = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_outgoing.sql', params)
        incoming = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_incoming.sql', params)

    cashflow_monthly_by_type_ts = process_ft_cashflow_monthly_by_type_ts(
        outgoing, incoming)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        cashflow_monthly_by_type_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)

def update_ft_suppliers_monthly_pv_ts():

    table = 'ft_suppliers_monthly_pv_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_opch.sql', params)

    with mysql_engine.connect() as mysql_conn:
        suppliers = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_suppliers_for_monthly_pv_ts.sql')

    suppliers_monthly_pv_ts = process_ft_suppliers_monthly_pv_ts(
        purchases, suppliers)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        suppliers_monthly_pv_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)

def update_ft_current_inventory():

    table = 'ft_current_inventory'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    with mssql_engine.connect() as mssql_conn:
        params = {'as_of_date': f"'{END_DATE_STR}'"}
        data = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/{table}.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{END_DATE_STR}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        data.to_sql(table, con=mysql_conn, if_exists='append',
                    index=False, chunksize=1000)

    record_data_refresh_log(table)

def update_ft_sales_agent_performance_ts():

    table = 'ft_sales_agent_performance_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE.replace(day=1) + relativedelta(months=-1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_credit_notes.sql', params)

    sales_agent_performance_ts = process_ft_sales_agent_performance_ts(
        sales, credit_notes)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        sales_agent_performance_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)
    record_data_refresh_log(table)


def update_ft_recent_sales():

    table = 'ft_recent_sales'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE + relativedelta(days=-3)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchase_prices = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    sales = process_ft_recent_sales(sales, purchase_prices)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-24)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-3)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        sales.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    
def update_ft_recent_purchases():

    table = 'ft_recent_purchases'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE + relativedelta(days=-3)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)

    purchases = process_ft_recent_purchases(purchases)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-24)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-3)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        purchases.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)

def update_ft_pdt_monthly_summary_ts():

    table = 'ft_pdt_monthly_summary_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE.replace(day=1) + relativedelta(months=-1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    pdt_monthly_summary_ts = process_ft_pdt_monthly_summary_ts(
        sales, purchases)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        pdt_monthly_summary_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False, chunksize=1000)

    record_data_refresh_log(table)

def update_ft_recent_ar_invoices():

    table = 'ft_recent_ar_invoices'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    days_of_data = 60
    start_date = END_DATE + relativedelta(days=-days_of_data)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_ar_invoices.sql', params)

    processed_invoices = process_ft_recent_ar_invoices(invoices)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-12)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        processed_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    
def update_ft_recent_ap_invoices():

    table = 'ft_recent_ap_invoices'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    days_of_data = 60
    start_date = END_DATE + relativedelta(days=-days_of_data)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_ap_invoices.sql', params)

    processed_invoices = process_ft_recent_ar_invoices(invoices)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-12)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)

    with mysql_engine.connect() as mysql_conn:
        processed_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)