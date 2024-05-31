
from utils.dbUtils import create_mssql_engine, create_mysql_engine, execute_in_mysql, get_data_from_query
from utils.dataProcessing import process_ft_cashflow_monthly_ts, process_ft_cashflow_monthly_by_type_ts, process_ft_suppliers_monthly_pv_ts, process_ft_sales_agent_performance_ts, process_ft_recent_sales, process_ft_recent_purchases, process_ft_pdt_monthly_summary_ts, process_ft_recent_ar_invoices, process_ft_recent_ap_invoices, process_ft_daily_qty_value_tracking_ts, process_ft_recent_credit_notes, process_ft_daily_sales_employee_value_ts, process_ft_daily_pdt_tracking_pdt_inv_value_ts, process_ft_daily_agg_values_ts, process_ft_daily_supplier_purchases_credit_notes_ts, process_ft_daily_supplier_ap_credit_notes_ts
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
               "ssl_ca": os.getenv('MYSQL_SSL_CA')}

RDS_CREDS = {"host": os.getenv('RDS_HOST'),
             "db_name": os.getenv('RDS_DB_NAME'),
             "db_user": os.getenv('RDS_DB_USER'),
             "db_pw": os.getenv('RDS_DB_PW')}

END_DATE = date.today()
END_DATE_STR = END_DATE.strftime("%Y-%m-%d")


def update_ft_cashflow_monthly_ts():

    table = 'ft_cashflow_monthly_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    start_date = END_DATE.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        outgoing = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_outgoing.sql', params)
        incoming = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_incoming.sql', params)

    cashflow_monthly_ts = process_ft_cashflow_monthly_ts(outgoing, incoming)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        cashflow_monthly_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def update_ft_cashflow_monthly_by_type_ts():

    table = 'ft_cashflow_monthly_by_type_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    start_date = END_DATE.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        outgoing = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_outgoing.sql', params)
        incoming = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_incoming.sql', params)

    cashflow_monthly_by_type_ts = process_ft_cashflow_monthly_by_type_ts(
        outgoing, incoming)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        cashflow_monthly_by_type_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def update_ft_suppliers_monthly_pv_ts():

    table = 'ft_suppliers_monthly_pv_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    start_date = END_DATE.replace(day=1)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/int_supplier_purchase_orders.sql', params)

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
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        suppliers_monthly_pv_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def update_ft_sales_agent_performance_ts():

    table = 'ft_sales_agent_performance_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date.replace(day=1) + relativedelta(months=-2)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        monthly_agg_sales = get_data_from_query(mssql_conn, f'./sql/mssql/init/ft_sales_agent_performance_ts.sql', params)
        customers = get_data_from_query(mssql_conn, f'./sql/mssql/init/dim_customers.sql')
        
    monthly_agg_sales_ts = process_ft_sales_agent_performance_ts(monthly_agg_sales, customers)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        monthly_agg_sales_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()

def update_ft_recent_sales():

    table = 'ft_recent_sales'
    days_of_data = 14 
    
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    
    start_date = END_DATE + relativedelta(days=-days_of_data)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)

    mssql_engine.dispose()
    
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
        mysql_conn.commit()
    
    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-13)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        sales.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()

def update_ft_recent_credit_notes():

    table = 'ft_recent_credit_notes'
    days_of_data = 14   

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    start_date = END_DATE + relativedelta(days=-days_of_data)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_credit_notes.sql', params)

    mssql_engine.dispose()
    credit_notes = process_ft_recent_credit_notes(credit_notes)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-13)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        credit_notes.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()


def update_ft_recent_purchases():

    table = 'ft_recent_purchases'
    days_of_data = 14   

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    start_date = END_DATE + relativedelta(days=-days_of_data)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)

    mssql_engine.dispose()
    purchases = process_ft_recent_purchases(purchases)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-13)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        purchases.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    

def update_ft_pdt_monthly_summary_ts():

    table = 'ft_pdt_monthly_summary_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    start_date = END_DATE.replace(day=1) + relativedelta(months=-3)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        
    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_inventory.sql', params)
        recent_price = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_purchase_prices.sql', params)

    pdt_monthly_summary_ts = process_ft_pdt_monthly_summary_ts(
        sales, purchases, inv, recent_price)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "agg_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        pdt_monthly_summary_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False, chunksize=1000)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def update_ft_recent_ar_invoices():

    table = 'ft_recent_ar_invoices'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    days_of_data = 14
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
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-12)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + \
            relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        processed_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()


def update_ft_recent_ap_invoices():

    table = 'ft_recent_ap_invoices'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    days_of_data = 14
    start_date = END_DATE + relativedelta(days=-days_of_data)
    start_date_str = start_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        invoices = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_ap_invoices.sql', params)

    processed_invoices = process_ft_recent_ap_invoices(invoices)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{start_date_str}'", "end_date": f"'{END_DATE_STR}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        removal_end_date = END_DATE + relativedelta(months=-12)
        removal_end_date_str = removal_end_date.strftime("%Y-%m-%d")
        removal_start_date = removal_end_date + \
            relativedelta(days=-days_of_data)
        removal_start_date_str = removal_start_date.strftime("%Y-%m-%d")
        params = {"table": f"{table}", "date_col": "doc_date",
                  "start_date": f"'{removal_start_date_str}'", "end_date": f"'{removal_end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        processed_invoices.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def update_ft_daily_qty_value_tracking_ts():

    table = 'ft_daily_qty_value_tracking_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
        products = get_data_from_query(mysql_conn, f'./sql/mysql/query/get_dim_pdts.sql')
        
    latest_date = latest_date.values[0][0]
    latest_date_str = latest_date.strftime("%Y-%m-%d")
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_sales.sql', params)
        purchases = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_current_purchases.sql', params)
        mssql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        inv = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_recent_inventory.sql', params)
        mysql_conn.commit()

    qty_value_ts = process_ft_daily_qty_value_tracking_ts(
        sales, inv, purchases, products, latest_date_str, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        qty_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)
        mysql_conn.commit()

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def update_ft_warehouse_inventory_ts():

    table = 'ft_warehouse_inventory_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
    
    latest_date = latest_date.values[0][0]
    latest_date = latest_date + relativedelta(days=-7)
    latest_date_str = latest_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    data_collate = []

    with mssql_engine.connect() as mssql_conn:
        for as_of_date in pd.date_range(latest_date, end_date, freq='d'):
            print(as_of_date)
            as_of_date_str = as_of_date.strftime("%Y-%m-%d")
            params = {'as_of_date': f"'{as_of_date_str}'"}
            data = get_data_from_query(
                mssql_conn, f'./sql/mssql/init/{table}.sql', params)
            data_collate.append(data)

    inv = pd.concat(data_collate, ignore_index=True)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        inv.to_sql(table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def update_ft_daily_sales_employee_value_ts():

    table = 'ft_daily_sales_employee_value_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
        
    latest_date = latest_date.values[0][0]
    latest_date_str = latest_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        sales_value_ts = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_sales_employee_value_ts.sql', params)

    sales_value_ts = process_ft_daily_sales_employee_value_ts(sales_value_ts)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        sales_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
def update_ft_daily_pdt_tracking_inv_value_ts():

    table = 'ft_daily_pdt_tracking_inv_value_ts'
    mysql_engine = create_mysql_engine(**RDS_CREDS)
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
        
    latest_date = latest_date.values[0][0]
    latest_date_str = latest_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_pdt_inv_value = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_daily_pdt_inv_value.sql', params)

    daily_pdt_inv_value_ts = process_ft_daily_pdt_tracking_pdt_inv_value_ts(daily_pdt_inv_value, latest_date_str, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        daily_pdt_inv_value_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    

def update_ft_daily_agg_values_ts():
    
    table = 'ft_daily_agg_values_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
        
    latest_date = latest_date.values[0][0]
    latest_date_str = latest_date.strftime("%Y-%m-%d")
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_agg_values = get_data_from_query(mssql_conn, f'./sql/mssql/query/ft_daily_agg_values_ts.sql', params)

    with mysql_engine.connect() as mysql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_inv_value = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_daily_inv_value_ts.sql', params)\

    daily_agg_values_ts = process_ft_daily_agg_values_ts(daily_agg_values, daily_inv_value, latest_date_str, end_date_str)

    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        daily_agg_values_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    
    
def update_ft_daily_supplier_purchases_credit_notes_ts():
    
        
    table = 'ft_daily_supplier_purchases_credit_notes_ts'

    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")    
    latest_date = latest_date.values[0][0]
    latest_date_str = latest_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        daily_agg_purchases_credit_notes = get_data_from_query(mssql_conn, f'./sql/mssql/init/ft_daily_supplier_purchases_credit_notes_ts.sql', params)

    with mysql_engine.connect() as mysql_conn:
        pdts = get_data_from_query(mysql_conn, f'./sql/mysql/query/get_dim_pdts.sql')
    
    daily_values_ts = process_ft_daily_supplier_purchases_credit_notes_ts(daily_agg_purchases_credit_notes, pdts)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

        
    with mysql_engine.connect() as mysql_conn:
        daily_values_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()
    

def update_ft_daily_supplier_ap_credit_notes_ts():
    
    table = 'ft_daily_supplier_ap_credit_notes_ts'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    mysql_engine = create_mysql_engine(**RDS_CREDS)

    with mysql_engine.connect() as mysql_conn:
        get_date_params = {"date_col": f"as_of_date",
                  "tbl": f"{table}"}
        latest_date = get_data_from_query(
            mysql_conn, f'./sql/mysql/query/get_latest_date_from_tbl.sql', get_date_params)
    
    end_date = date.today()
    end_date_str = end_date.strftime("%Y-%m-%d")    
    latest_date = latest_date.values[0][0]
    latest_date_str = latest_date.strftime("%Y-%m-%d")

    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{latest_date_str}'",
                  "end_date": f"'{end_date_str}'"}
        ap_credit_notes = get_data_from_query(
            mssql_conn, f'./sql/mssql/init/ft_daily_supplier_ap_credit_notes_ts.sql', params)

    ap_credit_notes_ts = process_ft_daily_supplier_ap_credit_notes_ts(ap_credit_notes)
    
    with mysql_engine.connect() as mysql_conn:
        params = {"table": f"{table}", "date_col": "as_of_date",
                  "start_date": f"'{latest_date_str}'", "end_date": f"'{end_date_str}'"}
        execute_in_mysql(
            mysql_conn, f'./sql/mysql/delete/delete_row_by_date_range.sql', params)
        mysql_conn.commit()

    with mysql_engine.connect() as mysql_conn:
        ap_credit_notes_ts.to_sql(
            table, con=mysql_conn, if_exists='append', index=False)

    record_data_refresh_log(table)
    mysql_engine.dispose()
    mssql_engine.dispose()