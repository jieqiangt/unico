from utils.dbUtils import create_mysql_engine,  execute_in_mysql, get_data_from_query, create_mssql_engine
from utils.dataProcessing import process_current_inventory_report, process_sales_pricing_report
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
               "ssl_ca": os.getenv('MYSQL_SSL_CA')}

RDS_CREDS = {"host": os.getenv('RDS_HOST'),
             "db_name": os.getenv('RDS_DB_NAME'),
             "db_user": os.getenv('RDS_DB_USER'),
             "db_pw": os.getenv('RDS_DB_PW')}

END_DATE = date.today()
END_DATE_STR = END_DATE.strftime("%Y-%m-%d")


def export_current_inventory_report():

    file_name = f'{END_DATE_STR}_current_inventory_report'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)

    with mssql_engine.connect() as mssql_conn:
        inv_with_price = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/current_inventory_report_with_price.sql')

    mssql_engine.dispose()

    cols_to_drop = ['avg_price', 'last_purchase_date',
                    'last_purchase_price', 'breakeven_price']
    inv = inv_with_price.drop(columns=cols_to_drop)

    process_current_inventory_report(inv, file_name)
    record_data_refresh_log(f'{file_name}_excel')


def export_sales_pricing_report():

    file_name = f'{END_DATE_STR}_sales_pricing_report'
    mssql_engine = create_mssql_engine(**MSSQL_CREDS)
    
    start_date = END_DATE.replace(day=1) + relativedelta(months=-12)
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    with mssql_engine.connect() as mssql_conn:
        params = {"start_date": f"'{start_date_str}'",
                  "end_date": f"'{END_DATE_STR}'"}
        pdt_industry_pc = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/int_pdt_industry_pc.sql', params)
        inv_with_price = get_data_from_query(
            mssql_conn, f'./sql/mssql/query/current_inventory_report_with_price.sql')
    
    mssql_engine.dispose()

    process_sales_pricing_report(
        inv_with_price, pdt_industry_pc, f"{file_name}")
    record_data_refresh_log(f'{file_name}_excel')