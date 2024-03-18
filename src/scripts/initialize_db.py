
from utils.dbUtils import create_mssql_engine, create_mysql_engine, execute_in_mysql, get_data_from_query, drop_table
import os
from dotenv import load_dotenv
from datetime import date
from dateutil.relativedelta import relativedelta
from utils.dataProcessing import process_ft_cashflow_monthly_ts, process_ft_cashflow_monthly_by_type_ts, process_ft_suppliers_monthly_pv_ts, process_ft_sales_agent_performance_ts, process_ft_recent_sales, process_ft_recent_purchases, process_ft_pdt_monthly_summary_ts, process_ft_recent_ar_invoices, process_ft_processed_pdt_daily_output_ts, process_ft_daily_qty_value_tracking_ts, process_ft_daily_sales_employee_value_ts, process_ft_daily_purchase_value_ts, process_ft_daily_pdt_processing_movement_ts
from utils.logging import record_data_refresh_log
import pandas as pd
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
               "ssl_ca": os.getenv('MYSQL_SSL_CA')}

RDS_CREDS = {"host": os.getenv('RDS_HOST'),
             "db_name": os.getenv('RDS_DB_NAME'),
             "db_user": os.getenv('RDS_DB_USER'),
             "db_pw": os.getenv('RDS_DB_PW')}


def init_dim_customers():

    table = 'dim_customers'
    cutoff_date = date.today().replace(day=1) + relativedelta(months=-24)
    cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
    
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mssql_engine.connect() as mssql_conn:
        params = {'cutoff_date': f"'{cutoff_date_str}'"}
        data = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/{table}.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        data.to_sql(table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_dim_suppliers(cutoff_date):

    table = 'dim_suppliers'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mssql_engine.connect() as mssql_conn:
        params = {'cutoff_date': f"'{cutoff_date}'"}
        data = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/{table}.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        data.to_sql(table, con=mysql_conn, if_exists='append',
                    index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_dim_pdts():

    table = 'dim_pdts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mssql_engine.connect() as mssql_conn:
        data = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        data.to_sql(table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_cashflow_monthly_ts():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    table = 'ft_cashflow_monthly_ts'
    with mssql_engine.connect() as mssql_conn:
        outgoing = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_outgoing.sql')
        incoming = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_incoming.sql')

    cashflow_monthly_ts = process_ft_cashflow_monthly_ts(outgoing, incoming)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        cashflow_monthly_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_cashflow_monthly_by_type_ts():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    table = 'ft_cashflow_monthly_by_type_ts'
    with mssql_engine.connect() as mssql_conn:
        outgoing = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_outgoing.sql')
        incoming = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_incoming.sql')

    cashflow_monthly_by_type_ts = process_ft_cashflow_monthly_by_type_ts(
        outgoing, incoming)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        cashflow_monthly_by_type_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_suppliers_monthly_pv_ts():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    table = 'ft_suppliers_monthly_pv_ts'
    with mssql_engine.connect() as mssql_conn:
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_supplier_purchase_orders.sql')

    with mysql_engine.connect() as mysql_conn:
        suppliers = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_suppliers_for_monthly_pv_ts.sql')

    supp_monthly_pv_ts = process_ft_suppliers_monthly_pv_ts(
        purchases, suppliers)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        supp_monthly_pv_ts.to_sql(table,
                                  con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()

def init_ft_warehouse_inventory_ts(start_date=None):

    table = 'ft_warehouse_inventory_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()

    if start_date is None:
        start_date = date.today().replace(day=1) + relativedelta(months=-36)
        # start_date = end_date + relativedelta(days=-5)

    data_collate = []

    with mssql_engine.connect() as mssql_conn:
        for as_of_date in pd.date_range(start_date, end_date, freq='d'):
            print(as_of_date)
            as_of_date_str = as_of_date.strftime("%Y-%m-%d")
            params = {'as_of_date': f"'{as_of_date_str}'"}
            data = get_data_from_query(
                mssql_conn, f'./sql/mssql/init/{table}.sql', params)
            data_collate.append(data)

    inv = pd.concat(data_collate, ignore_index=True)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        inv.to_sql(table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_sales_agent_performance_ts():

    table = 'ft_sales_agent_performance_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_olap_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = date(2020, 1, 1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        print('getting sales data..')
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_credit_notes.sql', params)

    with mysql_olap_engine.connect() as mysql_conn:
        # execute_in_mysql(
        #     mysql_conn, f'./sql/mysql/config/set_olap.sql')
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_prices = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    mysql_olap_engine.dispose()

    print('processing_sales_agent_performance...')
    sales_agent_performance_ts = process_ft_sales_agent_performance_ts(
        sales, purchase_prices, credit_notes)

    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        sales_agent_performance_ts.to_sql(table, con=mysql_conn, if_exists='append',
                                          index=False, chunksize=1000)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_recent_sales():

    table = 'ft_recent_sales'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_olap_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-6)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    with mysql_olap_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        # execute_in_mysql(
        #     mysql_conn, f'./sql/mysql/config/set_olap.sql')
        purchase_prices = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    mysql_olap_engine.dispose()

    sales = process_ft_recent_sales(sales, purchase_prices)

    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, end_date_str, freq='W'))

    if (date_range[-1].strftime("%Y-%m-%d") != end_date_str):
        date_range.append(end_date)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")

        date_condition = ((sales['doc_date'] >= append_start_date) & (
            sales['doc_date'] < append_end_date))

        if (append_end_date == end_date_str):
            date_condition = ((sales['doc_date'] >= append_start_date) & (
                sales['doc_date'] <= append_end_date))

        sales_to_append = sales[date_condition]

        with mysql_engine.connect() as mysql_conn:
            sales_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                   index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_recent_purchases():

    table = 'ft_recent_purchases'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)

    purchases = process_ft_recent_purchases(purchases)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        purchases.to_sql(table, con=mysql_conn, if_exists='append',
                         index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_pdt_monthly_summary_ts():

    table = 'ft_pdt_monthly_summary_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-48)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_inventory.sql', params)
        recent_price = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    pdt_monthly_summary_ts = process_ft_pdt_monthly_summary_ts(
        sales, purchases, inv, recent_price)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        pdt_monthly_summary_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_recent_ar_invoices():

    table = 'ft_recent_ar_invoices'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_ar_invoices.sql', params)

    processed_invoices = process_ft_recent_ar_invoices(ar_invoices)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, end_date_str, freq='W'))

    if (date_range[-1].strftime("%Y-%m-%d") != end_date_str):
        date_range.append(end_date)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")

        date_condition = ((processed_invoices['doc_date'] >= append_start_date) & (
            processed_invoices['doc_date'] < append_end_date))

        if (append_end_date == end_date_str):
            date_condition = ((processed_invoices['doc_date'] >= append_start_date) & (
                processed_invoices['doc_date'] <= append_end_date))

        processed_invoices_to_append = processed_invoices[date_condition]

        with mysql_engine.connect() as mysql_conn:
            processed_invoices_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                                index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_recent_ap_invoices():

    table = 'ft_recent_ap_invoices'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_ap_invoices.sql', params)

    processed_invoices = process_ft_recent_ar_invoices(ar_invoices)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, end_date_str, freq='W'))

    if (date_range[-1].strftime("%Y-%m-%d") != end_date_str):
        date_range.append(end_date)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")

        date_condition = ((processed_invoices['doc_date'] >= append_start_date) & (
            processed_invoices['doc_date'] < append_end_date))

        if (append_end_date == end_date_str):
            date_condition = ((processed_invoices['doc_date'] >= append_start_date) & (
                processed_invoices['doc_date'] <= append_end_date))

        processed_invoices_to_append = processed_invoices[date_condition]

        with mysql_engine.connect() as mysql_conn:
            processed_invoices_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                                index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_processed_pdt_daily_output_ts():

    table = 'ft_processed_pdt_daily_output_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        processed_pdt_output = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_processed_pdt_daily_output_ts.sql', params)

    processed_pdt_output = process_ft_processed_pdt_daily_output_ts(
        processed_pdt_output, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    date_range = list(pd.date_range(start_date_str, end_date_str, freq='W'))

    if (date_range[-1].strftime("%Y-%m-%d") != end_date_str):
        date_range.append(end_date)

    for date_range_counter in range(len(date_range)-1):

        append_start_date = date_range[date_range_counter].strftime("%Y-%m-%d")
        append_end_date = date_range[date_range_counter +
                                     1].strftime("%Y-%m-%d")

        date_condition = ((processed_pdt_output['doc_date'] >= append_start_date) & (
            processed_pdt_output['doc_date'] < append_end_date))

        if (append_end_date == end_date_str):
            date_condition = ((processed_pdt_output['doc_date'] >= append_start_date) & (
                processed_pdt_output['doc_date'] <= append_end_date))

        processed_pdt_output_to_append = processed_pdt_output[date_condition]

        with mysql_engine.connect() as mysql_conn:
            processed_pdt_output_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                                  index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_daily_qty_value_tracking_ts():

    table = 'ft_daily_qty_value_tracking_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    start_date = date.today().replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)
        price_params = {"as_of_date": f"'{end_date_str}'"}
        recent_price = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sap_avg_price.sql', price_params)

    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_inventory.sql', params)

    qty_value_ts = process_ft_daily_qty_value_tracking_ts(
        sales, inv, purchases, recent_price, start_date_str, end_date_str)

    with mysql_engine.connect() as mysql_conn:

        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        qty_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()

def init_ft_daily_sales_employee_value_ts():

    table = 'ft_daily_sales_employee_value_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    start_date = date.today().replace(day=1).replace(month=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales_value_ts = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_sales_employee_value_ts.sql', params)

    sales_value_ts = process_ft_daily_sales_employee_value_ts(sales_value_ts)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        sales_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_purchase_value_ts():

    table = 'ft_daily_purchase_value_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    start_date = date.today().replace(day=1).replace(month=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_value_ts = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_purchase_value_ts.sql', params)

    purchase_value_ts = process_ft_daily_purchase_value_ts(purchase_value_ts)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        purchase_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
    
def init_ft_daily_pdt_processing_movement_ts():

    table = 'ft_daily_pdt_processing_movement_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    start_date = date.today().replace(day=1).replace(month=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        processing_movement = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_pdt_processing_movement_ts.sql', params)

    processing_movement = process_ft_daily_pdt_processing_movement_ts(processing_movement)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        processing_movement.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    

