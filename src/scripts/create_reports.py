from utils.dbUtils import create_mysql_engine,  execute_in_mysql, get_data_from_query, create_mssql_engine
from utils.dataProcessing import process_sales_ops_report, process_procurement_ops_report, process_ft_pdt_summary, process_ft_purchases_alerts, process_ft_sales_orders_alerts, process_ft_pdt_loss_summary, process_int_pdt_purchase_price_ts
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import date
from dateutil.relativedelta import relativedelta
from utils.logging import record_data_refresh_log
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


def create_ft_sales_ops_report():

    table = 'ft_sales_ops_report'
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        pdt_base = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_sales_ops_report.sql')
        params = {'as_of_date': f"'{END_DATE_STR}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_current_inventory.sql', params)

    sales_report = process_sales_ops_report(pdt_base, inv)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        sales_report.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)


def create_ft_procurement_ops_report():

    table = 'ft_procurement_ops_report'
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        pdt_base = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_procurement_ops_report.sql')
        params = {'as_of_date': f"'{END_DATE_STR}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_current_inventory.sql', params)

    procurement_report = process_procurement_ops_report(pdt_base, inv)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        procurement_report.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)


def create_ft_pdt_summary():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_olap_engine = create_mysql_engine(**MYSQL_CREDS)

    table = 'ft_pdt_summary'
    sampled_months = 18

    end_date = END_DATE
    query_start_date = end_date.replace(day=1) + relativedelta(months=-24)
    sample_start_date = end_date.replace(
        day=1) + relativedelta(months=-sampled_months)

    query_start_date_str = query_start_date.strftime("%Y-%m-%d")
    sample_start_date_str = sample_start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{query_start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        products = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_pdt_base_price.sql', params)
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)

    with mysql_olap_engine.connect() as mysql_conn:
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/config/set_olap.sql')
        params = {"start_date": f"'{query_start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_prices = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    mysql_olap_engine.dispose()

    ft_pdt_summary = process_ft_pdt_summary(
        sales, purchases, products, purchase_prices, query_start_date_str, sample_start_date_str, end_date_str, sampled_months)

    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ft_pdt_summary.to_sql(table,
                              con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)


def create_ft_sales_orders_alerts():

    table = 'ft_sales_orders_alerts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_olap_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE + relativedelta(days=-14)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        current_sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        
    with mysql_olap_engine.connect() as mysql_conn:
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/config/set_olap.sql')
        pdt_summary = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_ft_sales_orders_alerts.sql')
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchase_prices = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    mysql_olap_engine.dispose()

    sales_report = process_ft_sales_orders_alerts(
        current_sales, pdt_summary, purchase_prices)

    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, END_DATE_STR, freq='W'))

    if (date_range[-1].strftime("%Y-%m-%d") != END_DATE_STR):
        date_range.append(END_DATE)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")

        date_condition = ((sales_report['doc_date'] >= append_start_date) & (
            sales_report['doc_date'] < append_end_date))

        if (append_end_date == END_DATE_STR):
            date_condition = ((sales_report['doc_date'] >= append_start_date) & (
                sales_report['doc_date'] <= append_end_date))
            
        sales_report_to_append = sales_report[date_condition]

        with mysql_engine.connect() as mysql_conn:
            sales_report_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                          index=False, chunksize=1000)

    record_data_refresh_log(table)


def create_ft_purchases_alerts():

    table = 'ft_purchases_alerts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    start_date = END_DATE + relativedelta(days=-14)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)

    with mysql_engine.connect() as mysql_conn:
        pdt_summary = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_ft_purchases_alerts.sql')

    purchases_alerts = process_ft_purchases_alerts(purchases, pdt_summary)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, END_DATE_STR, freq='W'))

    if (date_range[-1].strftime("%Y-%m-%d") != END_DATE_STR):
        date_range.append(END_DATE)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")

        date_condition = ((purchases_alerts['doc_date'] >= append_start_date) & (
            purchases_alerts['doc_date'] < append_end_date))
        
        if (append_end_date == END_DATE_STR):
            date_condition = ((purchases_alerts['doc_date'] >= append_start_date) & (
                purchases_alerts['doc_date'] <= append_end_date))

        purchases_alerts_to_append = purchases_alerts[date_condition]
        
        with mysql_engine.connect() as mysql_conn:
            purchases_alerts_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                              index=False, chunksize=1000)
    record_data_refresh_log(table)


def create_ft_pdt_loss_summary():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_olap_engine = create_mysql_engine(**MYSQL_CREDS)

    table = 'ft_pdt_loss_summary'

    end_date = END_DATE
    start_date = end_date.replace(day=1) + relativedelta(months=-12)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    with mysql_olap_engine.connect() as mysql_conn:
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/config/set_olap.sql')
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_prices = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    mysql_olap_engine.dispose()

    pdt_loss_summary = process_ft_pdt_loss_summary(
        sales, purchase_prices, start_date_str, END_DATE_STR)

    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        pdt_loss_summary.to_sql(table,
                                con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)


def create_int_pdt_purchase_price_ts():

    table = 'int_pdt_purchase_price_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-48)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_pdt_purchase_price_ts.sql', params)

    pdt_purchase_price_ts = process_int_pdt_purchase_price_ts(
        purchases, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, end_date_str, freq='MS'))

    if (date_range[-1].strftime("%Y-%m-%d") != end_date_str):
        date_range.append(end_date)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")
        
        date_condition = ((pdt_purchase_price_ts['as_of_date'] >= append_start_date) & (
            pdt_purchase_price_ts['as_of_date'] < append_end_date))
        
        if (append_end_date == end_date_str):
            date_condition = ((pdt_purchase_price_ts['as_of_date'] >= append_start_date) & (
                pdt_purchase_price_ts['as_of_date'] <= append_end_date))
        
        pdt_purchase_price_ts_to_append = pdt_purchase_price_ts[date_condition]

        with mysql_engine.connect() as mysql_conn:
            pdt_purchase_price_ts_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                                   index=False, chunksize=1000)

    record_data_refresh_log(table)

def create_ft_current_inv_value():

    table = 'ft_current_inv_value'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    as_of_date = date.today()
    as_of_date_str = as_of_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"as_of_date": f"'{as_of_date_str}'"}
        inv_value = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_current_inv_value.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        inv_value.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    
def create_ft_current_account_balances():

    table = 'ft_current_account_balances'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    as_of_date = date.today()
    as_of_date_str = as_of_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"as_of_date": f"'{as_of_date_str}'"}
        current_account_balances = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_current_account_balances.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        current_account_balances.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    
def create_ft_accounts_aging_ts():

    table = 'ft_accounts_aging_ts'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        accounts_aging = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_accounts_aging_ts.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        accounts_aging.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)

def create_ft_outstanding_ar_breakdown():

    table = 'ft_outstanding_ar_breakdown'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        oustanding_ar = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/{table}.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        oustanding_ar.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    
def create_ft_outstanding_ap_breakdown():

    table = 'ft_outstanding_ap_breakdown'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**MYSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        accounts_aging = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/{table}.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        accounts_aging.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)

def test_logging():

    table = 'testing'
    record_data_refresh_log(table)
