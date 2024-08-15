
from utils.dbUtils import create_mssql_engine, create_mysql_engine, execute_in_mysql, get_data_from_query, drop_table
import os
from dotenv import load_dotenv
from datetime import date
from dateutil.relativedelta import relativedelta
from utils.dataProcessing import process_ft_cashflow_monthly_ts, process_ft_cashflow_monthly_by_type_ts, process_ft_suppliers_monthly_pv_ts, process_ft_recent_sales, process_ft_recent_purchases, process_ft_pdt_monthly_summary_ts, process_ft_recent_ar_invoices, process_ft_processed_pdt_daily_output_ts, process_ft_daily_qty_value_tracking_ts, process_ft_recent_credit_notes, process_ft_daily_pdt_tracking_pdt_inv_value_ts, process_ft_daily_agg_values_ts, process_ft_sales_agent_performance_ts, process_ft_daily_customer_sales_ts, process_ft_daily_sales_employee_value_ts, process_ft_daily_customer_ar_credit_notes_ts, process_ft_recent_incoming_payments, process_ft_daily_supplier_purchases_ts, process_ft_daily_supplier_ap_credit_notes_ts, process_ft_recent_outgoing_payments, process_ft_daily_supplier_purchases_credit_notes_ts, get_lat_long_for_bp
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

END_DATE = date.today()
END_DATE_STR = END_DATE.strftime("%Y-%m-%d")


def init_dim_customers():

    table = 'dim_customers'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mssql_engine.connect() as mssql_conn:
        customers = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/{table}.sql')
        
    customers_with_lat_long = get_lat_long_for_bp(customers)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        customers_with_lat_long.to_sql(table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_dim_suppliers():

    table = 'dim_suppliers'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mssql_engine.connect() as mssql_conn:
        suppliers = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/{table}.sql')
        
    suppliers_with_lat_long = get_lat_long_for_bp(suppliers)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        suppliers_with_lat_long.to_sql(table, con=mysql_conn, if_exists='append',
                    index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()


def init_dim_pdts():

    table = 'dim_pdts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    start_date = END_DATE.replace(day=1) + relativedelta(months=-6)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        data = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/{table}.sql',params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        data.to_sql(table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()

def init_dim_dates():
    
    today = date.today()
    end_date = today + relativedelta(months=24)
    start_date = today + relativedelta(months=-48)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    dates = pd.DataFrame({"date_key": pd.date_range(start_date_str, end_date_str)})
    dates["start_of_month"] = dates['date_key'].to_numpy().astype('datetime64[M]')
    
    table = 'dim_dates'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        dates.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()

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

def init_ft_recent_sales():

    table = 'ft_recent_sales'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    with mysql_engine.connect() as mysql_conn:
        purchase_price = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_current_purchase_price.sql')

    sales = process_ft_recent_sales(sales, purchase_price)

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
    start_date = end_date.replace(day=1) + relativedelta(months=-12)
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
        current_price = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_current_purchase_price.sql')

    pdt_monthly_summary_ts = process_ft_pdt_monthly_summary_ts(
        sales, purchases, inv, current_price)

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


    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_inventory.sql', params)
        products = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_dim_pdts.sql')

    qty_value_ts = process_ft_daily_qty_value_tracking_ts(
        sales, inv, purchases, products, start_date_str, end_date_str)

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
    
def init_ft_recent_credit_notes():

    table = 'ft_recent_credit_notes'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_credit_notes.sql', params)

    credit_notes = process_ft_recent_credit_notes(credit_notes)

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

        date_condition = ((credit_notes['doc_date'] >= append_start_date) & (
            credit_notes['doc_date'] < append_end_date))

        if (append_end_date == end_date_str):
            date_condition = ((credit_notes['doc_date'] >= append_start_date) & (
                credit_notes['doc_date'] <= append_end_date))

        credit_notes_to_append = credit_notes[date_condition]

        with mysql_engine.connect() as mysql_conn:
            credit_notes_to_append.to_sql(table, con=mysql_conn, if_exists='append',
                                   index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_pdt_tracking_inv_value_ts():

    table = 'ft_daily_pdt_tracking_inv_value_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-18)
    start_date_str = start_date.strftime("%Y-%m-%d")

    
    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_pdt_inv_value = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_daily_pdt_inv_value.sql', params)

    daily_pdt_inv_value_ts = process_ft_daily_pdt_tracking_pdt_inv_value_ts(daily_pdt_inv_value, start_date_str, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        daily_pdt_inv_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    

def init_ft_daily_agg_values_ts():
    
    table = 'ft_daily_agg_values_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_agg_values = get_data_from_query(mssql_conn, f'./sql/mssql/query/ft_daily_agg_values_ts.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_inv_value = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_daily_inv_value_ts.sql', params)\

    daily_agg_values_ts = process_ft_daily_agg_values_ts(daily_agg_values, daily_inv_value, start_date_str, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        daily_agg_values_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_sales_agent_performance_ts():
    
    table = 'ft_sales_agent_performance_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-30)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        monthly_agg_sales = get_data_from_query(mssql_conn, f'./sql/mssql/init/ft_sales_agent_performance_ts.sql', params)
        customers = get_data_from_query(mssql_conn, f'./sql/mssql/init/dim_customers.sql')
        
    monthly_agg_sales_ts = process_ft_sales_agent_performance_ts(monthly_agg_sales, customers)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        monthly_agg_sales_ts.to_sql(
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
    
    
def init_ft_daily_customer_sales_ts():
    
    table = 'ft_daily_customer_sales_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_agg_sales = get_data_from_query(mssql_conn, f'./sql/mssql/init/ft_daily_customer_sales_ts.sql', params)
        customers = get_data_from_query(mssql_conn, f'./sql/mssql/init/dim_customers.sql')
        
    daily_agg_sales_ts = process_ft_daily_customer_sales_ts(daily_agg_sales, customers)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        daily_agg_sales_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_daily_customer_ar_credit_notes_ts():
    
    table = 'ft_daily_customer_ar_credit_notes_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_customer_ar_credit_notes_ts.sql', params)

    ar_credit_notes_ts = process_ft_daily_customer_ar_credit_notes_ts(ar_credit_notes)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ar_credit_notes_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()

def init_ft_recent_incoming_payments():
    
    table = 'ft_recent_incoming_payments'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        incoming_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_incoming_payments.sql', params)

    incoming_payments = process_ft_recent_incoming_payments(incoming_payments)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        incoming_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_ft_daily_supplier_purchases_ts():
    
    table = 'ft_daily_supplier_purchases_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_agg_purchases = get_data_from_query(mssql_conn, f'./sql/mssql/init/ft_daily_supplier_purchases_ts.sql', params)
        suppliers = get_data_from_query(mssql_conn, f'./sql/mssql/init/dim_suppliers.sql')
        
    daily_agg_purchases_ts = process_ft_daily_supplier_purchases_ts(daily_agg_purchases, suppliers)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        daily_agg_purchases_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_supplier_ap_credit_notes_ts():
    
    table = 'ft_daily_supplier_ap_credit_notes_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ap_credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_supplier_ap_credit_notes_ts.sql', params)

    ap_credit_notes_ts = process_ft_daily_supplier_ap_credit_notes_ts(ap_credit_notes)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ap_credit_notes_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()

def init_ft_recent_outgoing_payments():
    
    table = 'ft_recent_outgoing_payments'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        outgoing_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_outgoing_payments.sql', params)

    outgoing_payments = process_ft_recent_outgoing_payments(outgoing_payments)

    with mysql_engine.connect() as mysql_conn:
        params = {'table': table}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        outgoing_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_supplier_purchases_credit_notes_ts():
    
    table = 'ft_daily_supplier_purchases_credit_notes_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        suppliers = get_data_from_query(mssql_conn, f'./sql/mssql/init/dim_suppliers.sql')
        daily_agg_purchases_credit_notes = get_data_from_query(mssql_conn, f'./sql/mssql/init/ft_daily_supplier_purchases_credit_notes_ts.sql', params)

    with mysql_engine.connect() as mysql_conn:
        pdts = get_data_from_query(mysql_conn, f'./sql/mysql/query/get_dim_pdts.sql')
    
    daily_values_ts = process_ft_daily_supplier_purchases_credit_notes_ts(daily_agg_purchases_credit_notes, suppliers, pdts)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        daily_values_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_ar_ap_payment_diff_ts():
    
    table = 'ft_ar_ap_payment_diff_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-36)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_ap_diff = get_data_from_query(mssql_conn, f'./sql/mssql/query/ft_ar_ap_payment_diff_ts.sql', params)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/drop_table.sql', params)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')
        
    with mysql_engine.connect() as mysql_conn:
        ar_ap_diff.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def init_trs_ap_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_ap_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ap_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_ap_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ap_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_ar_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_ar_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_ar_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ar_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_credit_notes():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_credit_notes'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_credit_notes.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        credit_notes.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_incoming_payments():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_incoming_payments'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        incoming_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_incoming_payments.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        incoming_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_outgoing_payments():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_outgoing_payments'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        outgoing_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_outgoing_payments.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        outgoing_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_purchase_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_purchase_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_purchase_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        purchase_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_sales_orders():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_sales_orders'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales_orders = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_sales_orders.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        sales_orders.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_trs_ar_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'trs_ar_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/trs_ar_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ar_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_ap_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_ap_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ap_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_ap_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ap_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_ar_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_ar_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_ar_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ar_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_credit_notes():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_credit_notes'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_credit_notes.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        credit_notes.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_incoming_payments():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_incoming_payments'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        incoming_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_incoming_payments.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        incoming_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_outgoing_payments():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_outgoing_payments'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        outgoing_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_outgoing_payments.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        outgoing_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_purchase_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_purchase_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_purchase_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        purchase_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_daily_agg_sales_orders():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_daily_agg_sales_orders'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales_orders = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_daily_agg_sales_orders.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        sales_orders.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_ap_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_ap_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ap_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_ap_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ap_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_ar_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_ar_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ar_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_ar_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        ar_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_credit_notes():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_credit_notes'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_credit_notes.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        credit_notes.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_incoming_payments():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_incoming_payments'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        incoming_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_incoming_payments.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        incoming_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_outgoing_payments():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_outgoing_payments'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        outgoing_payments = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_outgoing_payments.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        outgoing_payments.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_purchase_invoices():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_purchase_invoices'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        purchase_invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_purchase_invoices.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        purchase_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()
    
def init_ft_monthly_agg_sales_orders():

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-60)
    start_date_str = start_date.strftime("%Y-%m-%d")

    table = 'ft_monthly_agg_sales_orders'
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales_orders = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/ft_monthly_agg_sales_orders.sql', params)

    with mysql_engine.connect() as mysql_conn:
        drop_table(mysql_conn, table)
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/create_table/{table}.sql')

    with mysql_engine.connect() as mysql_conn:
        sales_orders.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    mysql_engine.dispose()
    mssql_engine.dispose()