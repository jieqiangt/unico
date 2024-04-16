
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import relativedelta
from utils.excelUtils import auto_adjust_column, format_column_to_currency, add_borders_to_column, conditional_formatting_redfill, conditional_formatting_greenfill, format_column_to_percentage, format_column_to_date

from utils.pandaUtils import get_date_cols, convert_dt_cols, merge_all_df


def process_ft_cashflow_monthly_ts(outgoing, incoming):

    groupby_cols = [pd.Grouper(freq='MS')]
    num_cols = ["cash_sum", "credit_sum", "check_sum",
                "transfer_sum", "doc_total"]

    outgoing['doc_date'] = pd.to_datetime(outgoing['doc_date'])
    outgoing_ts_index = outgoing.set_index('doc_date')
    grouper_outgoing = outgoing_ts_index.groupby(groupby_cols)
    outgoing_sum = grouper_outgoing[num_cols].sum().reset_index()

    incoming['doc_date'] = pd.to_datetime(incoming['doc_date'])
    incoming_ts_index = incoming.set_index('doc_date')
    grouper_incoming = incoming_ts_index.groupby(groupby_cols)
    incoming_sum = grouper_incoming[num_cols].sum().reset_index()

    cashflow_monthly_ts = outgoing_sum[['doc_date', 'doc_total']].rename(columns={'doc_total': 'outgoing'}).merge(
        incoming_sum[['doc_date', 'doc_total']].rename(columns={'doc_total': 'incoming'}))
    cashflow_monthly_ts['diff'] = cashflow_monthly_ts['incoming'] - \
        cashflow_monthly_ts['outgoing']
    cashflow_monthly_ts['outgoing'] = cashflow_monthly_ts['outgoing'] * -1
    cashflow_monthly_ts.rename(columns={"doc_date": "agg_date"}, inplace=True)

    return cashflow_monthly_ts


def process_ft_cashflow_monthly_by_type_ts(outgoing, incoming):

    groupby_cols = [pd.Grouper(freq='MS')]
    num_cols = ["cash_sum", "credit_sum", "check_sum",
                "transfer_sum", "doc_total"]

    outgoing['doc_date'] = pd.to_datetime(outgoing['doc_date'])
    outgoing_ts_index = outgoing.set_index('doc_date')
    grouper_outgoing = outgoing_ts_index.groupby(groupby_cols)
    outgoing_sum = grouper_outgoing[num_cols].sum().reset_index()

    incoming['doc_date'] = pd.to_datetime(incoming['doc_date'])
    incoming_ts_index = incoming.set_index('doc_date')
    grouper_incoming = incoming_ts_index.groupby(groupby_cols)
    incoming_sum = grouper_incoming[num_cols].sum().reset_index()

    incoming_sum_long = pd.melt(incoming_sum, id_vars=[
                                'doc_date'], value_vars=num_cols, var_name='payment_type', value_name='incoming')
    outgoing_sum_long = pd.melt(outgoing_sum, id_vars=[
                                'doc_date'], value_vars=num_cols, var_name='payment_type', value_name='outgoing')

    cashflow_sum_long = incoming_sum_long.merge(
        outgoing_sum_long, on=['doc_date', 'payment_type'])
    cashflow_monthly_by_type_ts = cashflow_sum_long[cashflow_sum_long['payment_type'] != 'doc_total']
    cashflow_monthly_by_type_ts.rename(
        columns={"doc_date": "agg_date"}, inplace=True)

    return cashflow_monthly_by_type_ts


def process_ft_suppliers_monthly_pv_ts(purchases, suppliers):

    supp_pv = purchases.merge(
        suppliers, on='supplier_code', how='left')
    date_cols = get_date_cols(supp_pv)
    supp_pv = convert_dt_cols(supp_pv, date_cols)

    groupby_cols = [pd.Grouper(
        freq='MS'), 'overseas_local_ind', 'trade_ind', 'is_active']
    num_cols = ["doc_total"]

    supp_pv_ts_index = supp_pv.set_index('doc_date')
    grouper_pv = supp_pv_ts_index.groupby(groupby_cols)
    supp_monthly_pv_ts = grouper_pv[num_cols].sum().reset_index()

    supp_monthly_pv_ts.rename(
        columns={'doc_date': 'agg_date', 'doc_total': 'purchase_value'}, inplace=True)

    return supp_monthly_pv_ts


def calculate_pdt_summary(df, data_prefix, base=None, processed_qty=None):

    today = datetime.today()
    start_of_year_str = today.replace(
        month=1).replace(day=1).strftime('%Y-%m-%d')
    print(start_of_year_str)
    groupby_cols = ['pdt_code', 'pdt_name']

    max_date = df['doc_date'].max()
    min_date = df['doc_date'].min()

    date_delta = relativedelta.relativedelta(max_date, min_date)

    # get months difference
    sampled_months = date_delta.months + (date_delta.years * 12)

    if sampled_months == 0:
        sampled_months = 1

    # getting price summary statistics
    price_summary = df.groupby(groupby_cols)[
        ['price']].describe().droplevel(level=0, axis=1).reset_index()
    price_summary.rename(columns={"count": f"summary_{data_prefix}_order_count",
                                  "mean": f"summary_{data_prefix}_avg_price",
                                  "min": f"summary_{data_prefix}_min_price",
                                  "50%": f"summary_{data_prefix}_median_price",
                                  "max": f"summary_{data_prefix}_max_price",
                                  "25%": f"summary_{data_prefix}_25_percentile_price",
                                  "75%": f"summary_{data_prefix}_75_percentile_price",
                                  "std": f"summary_{data_prefix}_price_std"},
                         inplace=True)

    price_summary[f"summary_upper_{data_prefix}_price_iqr_limit"] = price_summary[f"summary_{data_prefix}_75_percentile_price"] + \
        (price_summary[f"summary_{data_prefix}_75_percentile_price"] -
         price_summary[f"summary_{data_prefix}_25_percentile_price"]) * 3

    price_summary[f"summary_lower_{data_prefix}_price_iqr_limit"] = price_summary[f"summary_{data_prefix}_75_percentile_price"] - \
        (price_summary[f"summary_{data_prefix}_75_percentile_price"] -
         price_summary[f"summary_{data_prefix}_25_percentile_price"]) * 3

    price_summary[f"summary_upper_{data_prefix}_price_std_limit"] = price_summary[f"summary_{data_prefix}_avg_price"] + \
        price_summary[f"summary_{data_prefix}_price_std"] * 6

    price_summary[f"summary_lower_{data_prefix}_price_std_limit"] = price_summary[f"summary_{data_prefix}_avg_price"] - \
        price_summary[f"summary_{data_prefix}_price_std"] * 6

    year_to_date_qty = df[df['doc_date'] >= start_of_year_str]
    year_to_date_qty = year_to_date_qty.groupby(['pdt_code', 'pdt_name'])[
        'qty'].sum().reset_index()
    year_to_date_qty.rename(columns={'qty': f'year_to_date_{
                            data_prefix}_qty'}, inplace=True)

    # getting num unique bp
    bp_map = {'sales': 'customer', 'purchases': 'supplier'}
    req_cols = ['pdt_code', 'pdt_name', f'{bp_map[data_prefix]}_code']
    num_bp = df[req_cols].drop_duplicates().groupby(
        ['pdt_code', 'pdt_name'])[f'{bp_map[data_prefix]}_code'].nunique().reset_index()
    num_bp.rename(columns={
                  f'{bp_map[data_prefix]}_code': f'num_{bp_map[data_prefix]}'}, inplace=True)

    if data_prefix == 'sales':

        # sales qty needs to be adjusted due to processed items

        # getting actual total sales quantity
        sales = df[['doc_date', 'pdt_code', 'pdt_name', 'qty', 'price']]
        sales["agg_date"] = sales['doc_date'].dt.to_period(
            'M').dt.to_timestamp()

        processed_qty["agg_date"] = processed_qty['doc_date'].dt.to_period(
            'M').dt.to_timestamp()

        sales_qty_per_month = sales.groupby(['agg_date', 'pdt_code', 'pdt_name'])[
            'qty'].sum().reset_index()
        processed_qty_per_month = processed_qty.groupby(['agg_date', 'pdt_code'])[
            'processed_qty'].sum().reset_index()

        actual_qty_per_month = sales_qty_per_month.merge(
            processed_qty_per_month, on=['agg_date', 'pdt_code'], how='left')
        actual_qty_per_month['processed_qty'] = actual_qty_per_month['processed_qty'].fillna(
            0)
        actual_qty_per_month['actual_qty'] = actual_qty_per_month['qty'] + \
            actual_qty_per_month['processed_qty']

        total_qty = actual_qty_per_month.groupby(['pdt_code', 'pdt_name'])[
            'actual_qty'].sum().reset_index()
        total_qty.rename(
            columns={"actual_qty": f"summary_{data_prefix}_total_qty"}, inplace=True)

        total_qty[f'summary_{data_prefix}_avg_qty_per_month'] = total_qty[f'summary_{
            data_prefix}_total_qty'] / sampled_months

        # getting actual total value
        avg_price_per_month = sales.groupby(['agg_date', 'pdt_code'])[
            'price'].mean().reset_index()

        total_value_per_month = actual_qty_per_month.merge(
            avg_price_per_month, on=['agg_date', 'pdt_code'], how='left')
        total_value_per_month['amount'] = total_value_per_month['actual_qty'] * \
            total_value_per_month['price']
        total_value = total_value_per_month.groupby(['pdt_code', 'pdt_name'])[
            'amount'].sum().reset_index()
        total_value.rename(
            columns={"amount": f"summary_{data_prefix}_total_value"}, inplace=True)

    else:
        # getting total value
        total_value = df.groupby(['pdt_code', 'pdt_name'])[
            'amount'].sum().reset_index()
        total_value.rename(
            columns={"amount": f"summary_{data_prefix}_total_value"}, inplace=True)

        # getting total quantity
        total_qty = df.groupby(['pdt_code', 'pdt_name'])[
            'qty'].sum().reset_index()
        total_qty.rename(
            columns={"qty": f"summary_{data_prefix}_total_qty"}, inplace=True)

        total_qty[f'summary_{data_prefix}_avg_qty_per_month'] = total_qty[f'summary_{
            data_prefix}_total_qty'] / sampled_months

    # getting latest date
    latest_date = df.copy()
    latest_date[f'summary_{data_prefix}_latest_date'] = df.groupby(
        ['pdt_code', 'pdt_name'])['doc_date'].transform('max')

    # getting latest price
    latest_price = latest_date[latest_date['doc_date'] == latest_date[f'summary_{data_prefix}_latest_date']].groupby(
        ['pdt_code', 'pdt_name'])['price'].mean().reset_index()
    latest_price.rename(
        columns={"price": f"summary_{data_prefix}_latest_price"}, inplace=True)

    latest_date = latest_date[[
        'pdt_code', 'pdt_name', f'summary_{data_prefix}_latest_date']].drop_duplicates()

    if base is None:
        summary = merge_all_df(
            dfs=[latest_date, latest_price, price_summary, total_qty, total_value, num_bp, year_to_date_qty], merge_keys=['pdt_code', 'pdt_name'])
        return summary

    summary = merge_all_df(
        dfs=[base, latest_price, latest_date, price_summary, total_qty, total_value, num_bp, year_to_date_qty], merge_keys=['pdt_code', 'pdt_name'])

    return summary


def process_ft_pdt_summary(sales, qty_processed, purchases, products, purchase_prices, query_start_date_str, purchase_sample_start_date_str, sales_sample_start_date_str, end_date_str, activity_cutoff_date_str):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)

    qty_processed_date_cols = get_date_cols(qty_processed)
    qty_processed = convert_dt_cols(qty_processed, qty_processed_date_cols)

    purchases_date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, purchases_date_cols)

    purchase_prices['as_of_date'] = pd.to_datetime(
        purchase_prices['as_of_date'])
    purchase_prices.drop(columns=['previous_price'], inplace=True)
    purchase_prices.rename(
        columns={'weighted_price': 'purchase_price'}, inplace=True)

    # filtering pdt giveaways
    sales = sales[sales['price'] > 0.01]

    # filtering out services
    sales = sales[~sales['pdt_code'].str.startswith('ZS')]
    purchases = purchases[~purchases['pdt_code'].str.startswith('ZS')]
    products = products[~products['pdt_code'].str.startswith('ZS')]

    # calculating sales summary
    base_sales = sales[['pdt_code', 'pdt_name']].drop_duplicates()

    sampled_sales = sales.loc[sales['doc_date'] >= sales_sample_start_date_str]
    sampled_qty_processed = qty_processed.loc[qty_processed['doc_date']
                                              >= sales_sample_start_date_str]

    # for pdts having sales within sampled timeframe
    sales_sampled_summary = calculate_pdt_summary(
        sampled_sales, 'sales', base_sales, sampled_qty_processed)
    sales_within_sample_summary = sales_sampled_summary[~sales_sampled_summary['summary_sales_latest_date'].isna(
    )]
    sales_within_sample_summary['summary_sales_start_date'] = sales_sample_start_date_str
    sales_within_sample_summary['summary_sales_end_date'] = end_date_str

    # for pdts having sales outside sampled timeframe
    sales_outside_sample = sales[sales['pdt_code'].isin(
        sales_sampled_summary.loc[sales_sampled_summary['summary_sales_latest_date'].isna(), 'pdt_code'])]
    qty_processed_outside_sample = qty_processed[qty_processed['pdt_code'].isin(
        sales_sampled_summary.loc[sales_sampled_summary['summary_sales_latest_date'].isna(), 'pdt_code'])]

    if not sales_outside_sample.empty:
        sales_outside_sample_summary = calculate_pdt_summary(
            sales_outside_sample, 'sales', base=None, processed_qty=qty_processed_outside_sample)
        sales_outside_sample_summary['summary_sales_start_date'] = query_start_date_str
        sales_outside_sample_summary['summary_sales_end_date'] = end_date_str

        sales_summary = pd.concat(
            [sales_within_sample_summary, sales_outside_sample_summary], ignore_index=True)
    else:
        sales_summary = sales_within_sample_summary.copy()

    # calculating purchases summary
    base_purchases = purchases[['pdt_code', 'pdt_name']].drop_duplicates()
    sampled_purchases = purchases.loc[purchases['doc_date']
                                      >= purchase_sample_start_date_str]

    # for pdts having purchases within sampled timeframe
    purchases_sampled_summary = calculate_pdt_summary(
        sampled_purchases, 'purchases', base_purchases)
    purchases_within_sample_summary = purchases_sampled_summary[
        ~purchases_sampled_summary['summary_purchases_latest_date'].isna()]
    purchases_within_sample_summary['summary_purchases_start_date'] = purchase_sample_start_date_str
    purchases_within_sample_summary['summary_purchases_end_date'] = end_date_str

    # for pdts having purchases outside sampled timeframe
    purchases_outside_sample = purchases[purchases['pdt_code'].isin(
        purchases_sampled_summary.loc[purchases_sampled_summary['summary_purchases_latest_date'].isna(), 'pdt_code'])]

    if not purchases_outside_sample.empty:
        purchases_outside_sample_summary = calculate_pdt_summary(
            purchases_outside_sample, f'purchases')
        purchases_outside_sample_summary['summary_purchases_start_date'] = query_start_date_str
        purchases_outside_sample_summary['summary_purchases_end_date'] = end_date_str

        purchases_summary = pd.concat(
            [purchases_within_sample_summary, purchases_outside_sample_summary], ignore_index=True)

    else:
        purchases_summary = purchases_within_sample_summary.copy()

    profit = sales.merge(
        purchase_prices, left_on=['doc_date', 'pdt_code'], right_on=['as_of_date', 'pdt_code'], how='left').merge(products, how='left', on=['pdt_code'])

    profit.loc[profit['purchase_price'].notnull(), 'profit_per_qty'] = profit['price'] - \
        profit['purchase_price']
    profit.loc[profit['purchase_price'].notnull(
    ), 'profit_calculated_by'] = 'purchase_price'
    profit.loc[profit['purchase_price'].isna(), 'profit_per_qty'] = profit['price'] - \
        profit['base_price']
    profit.loc[profit['purchase_price'].isna(
    ), 'profit_calculated_by'] = 'base_price'

    profit['total_profit'] = profit['profit_per_qty'] * profit['qty']
    profit['profit_margin'] = profit['profit_per_qty'] / profit['price']

    losses = profit[profit['total_profit'] <= 0]

    total_losses = losses[['pdt_code', 'total_profit']].groupby('pdt_code').sum(
    ).reset_index().rename(columns={'total_profit': 'total_loss'})
    total_losses['total_loss'] = np.abs(total_losses['total_loss'])

    num_loss_orders = losses[['pdt_code', 'profit_per_qty']].groupby('pdt_code').count(
    ).reset_index().rename(columns={'profit_per_qty': 'num_loss_orders'})

    pdt_profit = profit[['pdt_code', 'profit_per_qty', 'total_profit', 'profit_margin']].groupby(
        'pdt_code').agg({'profit_per_qty': 'median', 'total_profit': 'sum', 'profit_margin': 'median'}).reset_index()

    purchases_summary.drop(columns='pdt_name', inplace=True)
    sales_summary.drop(columns='pdt_name', inplace=True)

    # active_pdts = set(qty_processed[qty_processed['doc_date'] >= activity_cutoff_date_str]['pdt_code'].unique()).union(
    #               set(sales[sales['doc_date'] >= activity_cutoff_date_str]['pdt_code'].unique())).union(
    #               set(purchases[purchases['doc_date'] >= activity_cutoff_date_str]['pdt_code'].unique()))

    active_pdts = list(
        sales[sales['doc_date'] >= activity_cutoff_date_str]['pdt_code'].unique())

    pdt_activity = products[['pdt_code']].copy()
    pdt_activity.loc[pdt_activity['pdt_code'].isin(
        active_pdts), 'last_7_days_sales_is_active'] = 'active'
    pdt_activity.loc[~pdt_activity['pdt_code'].isin(
        active_pdts), 'last_7_days_sales_is_active'] = 'inactive'

    products = products[['pdt_code', 'pdt_name',
                         'uom', 'base_price', 'new_pdt_ind']]
    pdt_summary = products.merge(sales_summary, how='left', on=['pdt_code']).merge(
        purchases_summary, how='left', on=['pdt_code']).merge(
            pdt_profit, how='left', on=['pdt_code']).merge(
                num_loss_orders, how='left', on=['pdt_code']).merge(
                    total_losses, how='left', on=['pdt_code']).merge(
                    pdt_activity, how='left', on=['pdt_code']
    )

    return pdt_summary


def process_ft_sales_orders_alerts(current_sales, pdt_summary, purchase_prices, pdts):

    date_cols = get_date_cols(current_sales)
    current_sales = convert_dt_cols(current_sales, date_cols)

    current_sales = current_sales[current_sales['price'] > 0.01]
    purchase_prices['as_of_date'] = pd.to_datetime(
        purchase_prices['as_of_date'])

    pdts = pdts[['pdt_code', 'processed_pdt_ind']]

    sales_orders_alerts = current_sales.merge(
        pdt_summary, on=['pdt_code', 'pdt_name'], how='left').merge(
            purchase_prices, left_on=['doc_date', 'pdt_code'], right_on=['as_of_date', 'pdt_code'], how='left')
    sales_orders_alerts = sales_orders_alerts.merge(
        pdts, on='pdt_code', how='left')

    sales_orders_alerts['sales_price_alert'] = (
        ((sales_orders_alerts['price'] >
          sales_orders_alerts['summary_upper_sales_price_iqr_limit']) &
         (sales_orders_alerts['price'] >
          sales_orders_alerts['summary_upper_sales_price_std_limit'])) |
        ((sales_orders_alerts['price'] <
          sales_orders_alerts['summary_lower_sales_price_iqr_limit']) &
         (sales_orders_alerts['price'] <
          sales_orders_alerts['summary_lower_sales_price_std_limit']))
    )

    sales_orders_alerts['purchase_price_alert'] = (
        sales_orders_alerts['price'] - sales_orders_alerts['weighted_price']) < 0

    sales_orders_alerts.drop(
        columns=['as_of_date', 'previous_price'], inplace=True)
    sales_orders_alerts.rename(
        columns={'weighted_price': 'purchase_price'}, inplace=True)

    return sales_orders_alerts


def process_ft_purchases_alerts(purchases, pdt_summary):

    date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, date_cols)

    purchases.rename(columns={'price': 'purchases_price'}, inplace=True)
    purchases_alerts = purchases.merge(
        pdt_summary, on=['pdt_code'], how='left')

    purchases_alerts['price_diff_percentage'] = (purchases_alerts['purchases_price'] - purchases_alerts['summary_purchases_median_price']
                                                 )/purchases_alerts['summary_purchases_median_price']

    purchases_alerts['purchases_price_alert'] = (
                                                ((purchases_alerts['purchases_price'] >
                                                 purchases_alerts['summary_upper_purchases_price_iqr_limit']) &
                                                 (purchases_alerts['purchases_price'] >
                                                  purchases_alerts['summary_upper_purchases_price_std_limit'])) |
                                                ((purchases_alerts['purchases_price'] <
                                                 purchases_alerts['summary_lower_purchases_price_iqr_limit']) &
                                                 (purchases_alerts['purchases_price'] <
                                                 purchases_alerts['summary_lower_purchases_price_std_limit'])) |
                                                (purchases_alerts['price_diff_percentage'] >= 0.1)
    )

    return purchases_alerts


def process_sales_ops_report(products, pdt_stats, inv_value):

    products_req_cols = ['pdt_code', 'pdt_name', 'foreign_pdt_name', 'uom',
                         'processed_pdt_ind', 'new_pdt_ind', 'pdt_main_category', 'base_price', 'ecommerce_pdt_ind']
    active_products = products[products['is_active'] == 'Y'][products_req_cols]
    report = active_products.merge(pdt_stats, on='pdt_code', how='left').merge(
        inv_value, on='pdt_code', how="left")

    report['monthly_sales_qty_to_current_inv_ratio'] = report['current_inv_qty'] / \
        report['avg_monthly_sales_qty']
    report['sales_activity_category'] = 'NORMAL'
    report.loc[report['monthly_sales_qty_to_current_inv_ratio']
               < 0.5, 'sales_activity_category'] = 'REORDER'
    report.loc[report['monthly_sales_qty_to_current_inv_ratio']
               >= 4, 'sales_activity_category'] = 'SLOW SALES'
    report.loc[report['monthly_sales_qty_to_current_inv_ratio'].isna(),
               'sales_activity_category'] = 'NO SALES'

    report.loc[report['processed_pdt_ind'] ==
               1, 'processed_pdt_ind'] = 'PROCESSED'
    report.loc[report['processed_pdt_ind']
               == 0, 'processed_pdt_ind'] = 'NORMAL'

    report['current_inv_qty'] = report['current_inv_qty'].fillna(0)
    report['current_inv_value'] = report['current_inv_value'].fillna(0)
    report['monthly_sales_qty_to_current_inv_ratio'] = report['monthly_sales_qty_to_current_inv_ratio'].fillna(
        0)
    report['avg_weekly_sales_qty'] = report['avg_monthly_sales_qty'] * 12 / 52
    report['avg_daily_sales_qty'] = report['avg_monthly_sales_qty'] * 12 / 365

    return report


def process_procurement_ops_report(pdt_base, inv):

    inv = inv[['pdt_code', 'current_inv_qty']].copy()
    inv.rename(
        columns={"current_inv_qty": "current_inv"}, inplace=True)

    report = pdt_base.merge(inv, on=['pdt_code'], how="left")

    renamed_columns = {'summary_purchases_date_range': 'purchases_data_date_range',
                       'summary_purchases_latest_date': 'latest_purchases_date',
                       'summary_purchases_latest_price': 'latest_purchases_price',
                       'summary_purchases_min_price': 'purchases_min_price',
                       'summary_sales_date_range': 'sales_data_date_range',
                       'summary_sales_latest_date': 'latest_sales_date',
                       'summary_sales_latest_price': 'latest_sales_price',
                       'summary_sales_min_price': 'sales_min_price',
                       'summary_sales_max_price': 'sales_max_price',
                       'summary_sales_median_price': 'sales_avg_price',
                       'summary_purchases_total_qty': 'total_purchases_qty',
                       'summary_sales_total_qty': 'total_sales_qty',
                       'summary_sales_avg_qty_per_month': 'avg_sales_qty_per_month'}
    report.rename(columns=renamed_columns, inplace=True)

    cols_seq = ['pdt_code',
                'pdt_name',
                'current_inv',
                'profit_margin',
                'profit_per_qty',
                'purchases_data_date_range',
                'latest_purchases_date',
                'latest_purchases_price',
                'purchases_min_price',
                'sales_data_date_range',
                'latest_sales_date',
                'latest_sales_price',
                'sales_max_price',
                'sales_avg_price',
                'sales_min_price',
                'total_purchases_qty',
                'total_sales_qty',
                'avg_sales_qty_per_month',
                'total_profit'
                ]

    return report[cols_seq]

def process_ft_recent_sales(sales, purchase_prices):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)

    purchase_prices_date_cols = get_date_cols(purchase_prices)
    purchase_prices = convert_dt_cols(
        purchase_prices, purchase_prices_date_cols)

    sales = sales[sales['price'] > 0.01]

    sales["agg_date"] = sales['doc_date'].dt.to_period('M').dt.to_timestamp()
    purchase_prices['as_of_date'] = pd.to_datetime(
        purchase_prices['as_of_date'])
    purchase_prices.rename(columns={'as_of_date': 'doc_date'}, inplace=True)

    sales_with_purchase_price = sales.merge(
        purchase_prices, on=['doc_date', 'pdt_code'], how='left')

    sales_with_purchase_price.rename(
        columns={'weighted_price': 'purchase_price'}, inplace=True)
    sales_with_purchase_price.drop(columns=['previous_price'], inplace=True)

    sales_with_purchase_price['pc1'] = (sales_with_purchase_price['price'] -
                                        sales_with_purchase_price['purchase_price'])/sales_with_purchase_price['price']

    return sales_with_purchase_price


def process_ft_recent_credit_notes(credit_notes):

    date_cols = get_date_cols(credit_notes)
    credit_notes = convert_dt_cols(credit_notes, date_cols)

    return credit_notes


def process_ft_recent_purchases(purchases):

    date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, date_cols)

    purchases["agg_date"] = purchases['doc_date'].dt.to_period(
        'M').dt.to_timestamp()

    return purchases


def process_ft_pdt_monthly_summary_ts(sales, purchases, inv, recent_price):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    purchases_date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, purchases_date_cols)
    inv_date_cols = get_date_cols(inv)
    inv = convert_dt_cols(inv, inv_date_cols)
    recent_price_date_cols = get_date_cols(recent_price)
    recent_price = convert_dt_cols(recent_price, recent_price_date_cols)

    sales = sales[sales['price'] > 0.01]
    sales["agg_date"] = sales['doc_date'].dt.to_period('M').dt.to_timestamp()
    purchases["agg_date"] = purchases['doc_date'].dt.to_period(
        'M').dt.to_timestamp()
    inv["agg_date"] = inv['as_of_date'].dt.to_period(
        'M').dt.to_timestamp()
    recent_price["agg_date"] = recent_price['as_of_date'].dt.to_period(
        'M').dt.to_timestamp()
    recent_price.rename(columns={'weighted_price': 'price'}, inplace=True)
    inv = inv.merge(recent_price[['as_of_date', 'pdt_code', 'price']], on=[
                    'as_of_date', 'pdt_code'])

    groupby_cols = [pd.Grouper(freq='MS'), 'pdt_code']
    price_req_cols = ['agg_date', 'pdt_code', 'price']
    price_col = ["price"]
    qty_req_cols = ['agg_date', 'pdt_code', 'qty']
    qty_col = ["qty"]

    sales_price = sales[price_req_cols].copy()
    sales_price_ts_index = sales_price.set_index('agg_date')
    grouper_sales_price = sales_price_ts_index.groupby(groupby_cols)
    sales_price_ts = grouper_sales_price[price_col].median().reset_index()

    sales_qty = sales[qty_req_cols].copy()
    sales_qty_ts_index = sales_qty.set_index('agg_date')
    grouper_sales_qty = sales_qty_ts_index.groupby(groupby_cols)
    sales_qty_ts = grouper_sales_qty[qty_col].sum().reset_index()

    sales_summary_ts = merge_all_df(
        dfs=[sales_price_ts, sales_qty_ts], merge_keys=['agg_date', 'pdt_code'])
    sales_summary_ts['transaction_type'] = 'sales'

    purchases_price = purchases[price_req_cols].copy()
    purchases_price_ts_index = purchases_price.set_index('agg_date')
    grouper_purchases_price = purchases_price_ts_index.groupby(groupby_cols)
    purchases_price_ts = grouper_purchases_price[price_col].median(
    ).reset_index()

    purchases_qty = purchases[qty_req_cols].copy()
    purchases_qty_ts_index = purchases_qty.set_index('agg_date')
    grouper_purchases_qty = purchases_qty_ts_index.groupby(groupby_cols)
    purchases_qty_ts = grouper_purchases_qty[qty_col].sum().reset_index()

    purchases_summary_ts = merge_all_df(
        dfs=[purchases_price_ts, purchases_qty_ts], merge_keys=['agg_date', 'pdt_code'])
    purchases_summary_ts['transaction_type'] = 'purchases'

    inv_price = inv[price_req_cols].copy()
    inv_price_ts_index = inv_price.set_index('agg_date')
    grouper_inv_price = inv_price_ts_index.groupby(groupby_cols)
    inv_price_ts = grouper_inv_price[price_col].median(
    ).reset_index()

    inv_qty = inv[qty_req_cols].copy()
    inv_qty_ts_index = inv_qty.set_index('agg_date')
    grouper_inv_qty = inv_qty_ts_index.groupby(groupby_cols)
    inv_qty_ts = grouper_inv_qty[qty_col].last().reset_index()

    inv_summary_ts = merge_all_df(
        dfs=[inv_price_ts, inv_qty_ts], merge_keys=['agg_date', 'pdt_code'])
    inv_summary_ts['transaction_type'] = 'inv'

    pdt_monthly_summary_ts = pd.concat(
        [sales_summary_ts, purchases_summary_ts, inv_summary_ts], ignore_index=True)

    return pdt_monthly_summary_ts


def process_int_pdt_purchase_price_ts(purchase_prices, end_date_str):

    date_cols = get_date_cols(purchase_prices)
    purchase_prices = convert_dt_cols(purchase_prices, date_cols)

    pdt_codes = purchase_prices['pdt_code'].unique()
    purchase_prices_qty = purchase_prices.groupby(['pdt_code', 'doc_date'])[
        'qty'].sum().reset_index()
    purchase_prices_price = purchase_prices.groupby(['pdt_code', 'doc_date'])[
        'price'].mean().reset_index()
    purchase_prices = purchase_prices_qty.merge(
        purchase_prices_price, how='left', on=['pdt_code', 'doc_date'])

    resampled_prices = []

    for pdt_code in pdt_codes:

        tmp_prices = purchase_prices[purchase_prices['pdt_code'] == pdt_code]
        tmp_prices_qty = tmp_prices.groupby(
            'doc_date')['qty'].sum().reset_index()
        tmp_prices_price = tmp_prices.groupby(
            'doc_date')['price'].mean().reset_index()
        tmp_prices = tmp_prices_qty.merge(
            tmp_prices_price, how='left', on='doc_date')
        tmp_prices.sort_values(by='doc_date', ascending=True, inplace=True)

        # tmp_prices['qty_ewma'] = tmp_prices['qty'].ewm(alpha=0.75).mean()
        tmp_prices['qty_rolling_cum_sum'] = tmp_prices['qty'].rolling(
            window=7, min_periods=1).sum()
        tmp_prices['weight'] = np.clip(
            (tmp_prices['qty'] / tmp_prices['qty_rolling_cum_sum']) * 1.2, a_min=0, a_max=1)
        tmp_prices['weight_for_previous'] = 1 - tmp_prices['weight']
        tmp_prices['weighted_portion_of_new_price'] = tmp_prices['weight'] * \
            tmp_prices['price']

        doc_date_col = tmp_prices['doc_date'].to_list()
        qty_rolling_cum_sum_col = tmp_prices['qty_rolling_cum_sum'].to_list()

        weighted_price_col = []
        previous_price_col = []
        previous_price = 0
        weighted_price = 0

        for weighted_portion_of_new_price, weight_for_previous in zip(tmp_prices['weighted_portion_of_new_price'].to_list(), tmp_prices['weight_for_previous'].to_list()):

            if previous_price == 0:
                previous_price = weighted_portion_of_new_price
                weighted_price = previous_price
            else:
                previous_price = weighted_price
                weighted_price = weighted_portion_of_new_price + \
                    weight_for_previous * previous_price

            previous_price_col.append(previous_price)
            weighted_price_col.append(weighted_price)

        data = {
            'as_of_date': doc_date_col,
            'previous_price': previous_price_col,
            'weighted_price': weighted_price_col,
            'qty_rolling_cum_sum': qty_rolling_cum_sum_col,
        }

        calculated_prices = pd.DataFrame(data=data)

        calculated_prices_index = calculated_prices.set_index('as_of_date')

        min_date = calculated_prices_index.index.min()
        date_range = pd.date_range(min_date, end_date_str)

        calculated_weighted_prices = calculated_prices_index[[
            'previous_price', 'weighted_price', 'qty_rolling_cum_sum']].resample('D', origin='start_day').ffill().reindex(date_range).fillna(method='ffill')

        calculated_weighted_prices['pdt_code'] = pdt_code
        calculated_weighted_prices.reset_index(inplace=True)
        calculated_weighted_prices.rename(
            columns={'index': 'as_of_date'}, inplace=True)
        resampled_prices.append(calculated_weighted_prices)

    col_seq = ['as_of_date', 'pdt_code', 'weighted_price', 'previous_price',
               'qty_rolling_cum_sum']
    weighted_prices_output = pd.concat(
        resampled_prices, ignore_index=True)[col_seq]

    return weighted_prices_output


def process_ft_pdt_loss_summary(sales, purchase_prices, start_date_str, end_date_str):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    purchase_prices_date_cols = get_date_cols(purchase_prices)
    purchase_prices = convert_dt_cols(
        purchase_prices, purchase_prices_date_cols)

    purchase_prices.rename(
        columns={'weighted_price': 'purchase_price'}, inplace=True)
    purchase_prices.drop(columns=['previous_price'], inplace=True)

    sales_req_cols = ['doc_date', 'pdt_code', 'pdt_name', 'qty', 'price']
    sales = sales[sales_req_cols]
    sales.rename(columns={'doc_date': 'as_of_date'}, inplace=True)

    profit = sales.merge(purchase_prices, on=[
                         'as_of_date', 'pdt_code'], how='left')

    profit['profit_loss_per_qty'] = profit['price'] - profit['purchase_price']
    profit['profit_loss'] = profit['profit_loss_per_qty'] * profit['qty']
    losses = profit[profit['profit_loss'] <= 0]

    total_profit = profit.groupby('pdt_code')['profit_loss'].sum(
    ).reset_index().rename(columns={"profit_loss": "total_profit_loss"})

    num_losses = losses.groupby('pdt_code')['profit_loss'].count(
    ).reset_index().rename(columns={"profit_loss": "num_losses"})

    total_losses = losses.groupby('pdt_code')['profit_loss'].sum(
    ).reset_index().rename(columns={"profit_loss": "total_losses"})
    total_losses['total_losses'] = np.abs(total_losses['total_losses'])

    pdt_loss_summary = total_profit.merge(
        num_losses, how='left', on=['pdt_code']).merge(
        total_losses, how='left', on=['pdt_code'])

    pdt_loss_summary['sample_start_date'] = start_date_str
    pdt_loss_summary['sample_end_date'] = end_date_str
    pdt_loss_summary = pdt_loss_summary[~pdt_loss_summary['pdt_code'].str.startswith(
        'ZS')]

    numeric_cols = ['total_profit_loss', 'num_losses', 'total_losses']

    for col in numeric_cols:
        pdt_loss_summary[col] = pdt_loss_summary[col].fillna(0)

    return pdt_loss_summary


def process_ft_recent_ar_invoices(invoices):

    date_cols = ['doc_date']
    invoices = convert_dt_cols(invoices, date_cols)
    invoices['agg_date'] = pd.to_datetime(invoices['agg_date'])

    return invoices


def process_ft_recent_ap_invoices(invoices):

    date_cols = ['doc_date']
    invoices = convert_dt_cols(invoices, date_cols)
    invoices['agg_date'] = pd.to_datetime(invoices['agg_date'])

    return invoices


def process_ft_customer_group_price_check_flagged_orders(flagged_orders):

    date_cols = get_date_cols(flagged_orders)
    flagged_orders = convert_dt_cols(flagged_orders, date_cols)

    return flagged_orders

def process_ft_processed_pdt_daily_output_ts(processed_pdt_output, end_date_str):

    date_cols = get_date_cols(processed_pdt_output)
    processed_pdt_output = convert_dt_cols(processed_pdt_output, date_cols)

    pdt_codes = processed_pdt_output['pdt_code'].unique()
    processed_pdt_output_indexed = processed_pdt_output.set_index('doc_date')

    pdt_outputs = []

    for pdt_code in pdt_codes:

        tmp_pdt_output = processed_pdt_output_indexed[processed_pdt_output_indexed['pdt_code'] == pdt_code]
        min_date = tmp_pdt_output.index.min()
        date_range = pd.date_range(min_date, end_date_str)

        tmp_pdt_output = tmp_pdt_output.resample(
            rule='D').sum().reindex(date_range)
        tmp_pdt_output['pdt_code'] = pdt_code
        tmp_pdt_output['qty'] = tmp_pdt_output['qty'].fillna(0)
        tmp_pdt_output['value'] = tmp_pdt_output['value'].fillna(0)

        pdt_outputs.append(tmp_pdt_output)

    resampled_processed_pdt_output = pd.concat(pdt_outputs)
    resampled_processed_pdt_output.reset_index(inplace=True)
    resampled_processed_pdt_output.rename(
        columns={'index': 'doc_date'}, inplace=True)
    resampled_processed_pdt_output['agg_date'] = resampled_processed_pdt_output['doc_date'].dt.to_period(
        'M').dt.to_timestamp()

    return resampled_processed_pdt_output


def process_ft_daily_qty_value_tracking_ts(sales, inv, purchases, products, start_date_str, end_date_str):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    purchases_date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, purchases_date_cols)
    inv_date_cols = get_date_cols(inv)
    inv = convert_dt_cols(inv, inv_date_cols)

    sales_req_cols = ['as_of_date', 'pdt_code', 'qty', 'price']
    purchases_req_cols = ['as_of_date', 'pdt_code', 'qty', 'price']

    sales = sales[~sales['pdt_code'].str.startswith('ZS')]
    sales = sales[sales['price'] > 0.01]
    sales.rename(columns={'doc_date': 'as_of_date'}, inplace=True)
    sales = sales[sales_req_cols]

    purchases.rename(columns={'doc_date': 'as_of_date'}, inplace=True)
    purchases = purchases[purchases_req_cols]

    sales = sales.set_index('as_of_date')
    purchases = purchases.set_index('as_of_date')

    date_range = pd.date_range(start_date_str, end_date_str)

    result_collate = []

    for pdt_code in inv['pdt_code'].unique():

        tmp_sales = sales[sales['pdt_code'] == pdt_code]
        tmp_inv = inv[inv['pdt_code'] == pdt_code]
        tmp_purchases = purchases[purchases['pdt_code'] == pdt_code]

        if not tmp_sales.empty:
            daily_tmp_sales = tmp_sales.resample(
                'D', origin='start_day').sum().reindex(date_range).fillna(0)
            daily_tmp_sales.reset_index(inplace=True)
            daily_tmp_sales['pdt_code'] = pdt_code
            daily_tmp_sales.rename(
                columns={'index': 'as_of_date'}, inplace=True)
            daily_tmp_sales['value'] = daily_tmp_sales['qty'] * \
                daily_tmp_sales['price']

            daily_tmp_sales_value = daily_tmp_sales.drop(
                columns=['qty', 'price'])
            daily_tmp_sales_value['value_type'] = 'sales_value'

            daily_tmp_sales_qty = daily_tmp_sales.drop(
                columns=['price', 'value'])
            daily_tmp_sales_qty['value_type'] = 'sales_qty'
            daily_tmp_sales_qty.rename(columns={'qty': 'value'}, inplace=True)

            result_collate.append(daily_tmp_sales_qty)
            result_collate.append(daily_tmp_sales_value)

        if not tmp_purchases.empty:
            daily_tmp_purchases = tmp_purchases.resample(
                'D', origin='start_day').sum().reindex(date_range).fillna(0)
            daily_tmp_purchases.reset_index(inplace=True)
            daily_tmp_purchases['pdt_code'] = pdt_code
            daily_tmp_purchases.rename(
                columns={'index': 'as_of_date'}, inplace=True)
            daily_tmp_purchases['value'] = daily_tmp_purchases['qty'] * \
                daily_tmp_purchases['price']

            daily_tmp_purchases_value = daily_tmp_purchases.drop(
                columns=['qty', 'price'])
            daily_tmp_purchases_value['value_type'] = 'purchase_value'

            daily_tmp_purchases_qty = daily_tmp_purchases.drop(
                columns=['price', 'value'])
            daily_tmp_purchases_qty['value_type'] = 'purchase_qty'
            daily_tmp_purchases_qty.rename(
                columns={'qty': 'value'}, inplace=True)

            result_collate.append(daily_tmp_purchases_value)
            result_collate.append(daily_tmp_purchases_qty)

        pdt_price = products.loc[products['pdt_code'] ==
                                 pdt_code, 'warehouse_calculated_avg_price'].item()

        tmp_inv_value = tmp_inv.copy()
        tmp_inv_value['value'] = tmp_inv_value['qty'] * pdt_price
        tmp_inv_value.drop(columns=['qty'], inplace=True)
        tmp_inv_value['value_type'] = 'inv_value'

        tmp_inv_qty = tmp_inv.copy()
        tmp_inv_qty['value_type'] = 'inv_qty'
        tmp_inv_qty.rename(columns={'qty': 'value'}, inplace=True)

        result_collate.append(tmp_inv_value)
        result_collate.append(tmp_inv_qty)

    qty_value_ts = pd.concat(result_collate, ignore_index=True)

    return qty_value_ts


def process_ft_customer_churn(customers, sales, last_month_str, two_weeks_before_str):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    last_month_sales = sales[sales['doc_date'] >= last_month_str].copy()
    last_2_weeks_sales = sales[sales['doc_date']
                               >= two_weeks_before_str].copy()

    active_customers = customers.loc[customers['is_active'] == 'Y', :][[
        'customer_code', 'name']]
    active_customers.rename(columns={'name': 'customer_name'}, inplace=True)

    last_2_months_active_customers = sales['customer_code'].unique()
    last_month_active_customers = last_month_sales['customer_code'].unique()
    last_2_weeks_active_customers = last_2_weeks_sales['customer_code'].unique(
    )

    active_customers.loc[active_customers['customer_code'].isin(
        last_2_months_active_customers), 'activity'] = 'Bought Last 12 Months'
    active_customers.loc[active_customers['customer_code'].isin(
        last_month_active_customers), 'activity'] = 'Bought Last Month'
    active_customers.loc[active_customers['customer_code'].isin(
        last_2_weeks_active_customers), 'activity'] = 'Bought Last 2 Weeks'
    active_customers.loc[active_customers['activity']
                         == 'nan', 'activity'] = 'Inactive'

    return active_customers


def process_ft_pdt_potential_customers(customers, pdts, sales):

    all_pdt_codes = list(pdts.loc[:, 'pdt_code'])

    all_customer_codes = set(
        customers.loc[customers['is_active'] == 'Y', 'customer_code'])
    current_pdt_customers = sales[['pdt_code', 'customer_code']].drop_duplicates(
        subset=['pdt_code', 'customer_code'])
    customer_total_sales = sales.groupby(['customer_code'])[
        'amount'].sum().reset_index()
    customer_total_sales.rename(
        columns={'amount': 'total_sales_value'}, inplace=True)
    customer_sales_order_num = sales.groupby(['customer_code'])[
        'amount'].count().reset_index()
    customer_sales_order_num.rename(
        columns={'amount': 'num_sales_orders'}, inplace=True)
    customer_sales_median = sales.groupby(['customer_code'])[
        'amount'].median().reset_index()
    customer_sales_median.rename(
        columns={'amount': 'median_sales_value'}, inplace=True)

    df_collate = []

    for pdt_code in all_pdt_codes:

        customer_list = set(
            current_pdt_customers.loc[current_pdt_customers['pdt_code'] == pdt_code, 'customer_code'])

        temp_potential_customers = all_customer_codes.copy()

        if bool(customer_list):
            temp_potential_customers = all_customer_codes - customer_list

        temp_df = pd.DataFrame(
            data={'customer_code': list(temp_potential_customers)})
        temp_df = temp_df.merge(customer_total_sales, on='customer_code').merge(
            customer_sales_order_num, on='customer_code').merge(customer_sales_median, on='customer_code')
        temp_df.sort_values(by='median_sales_value',
                            ascending=True, inplace=True)
        temp_df = temp_df.head(150)

        temp_df.loc[:, 'pdt_code'] = pdt_code
        df_collate.append(temp_df)

    potential_customers = pd.concat(df_collate, ignore_index=True)

    return potential_customers


def process_ft_current_processing_movement(processing_movement):

    date_cols = get_date_cols(processing_movement)
    processing_movement = convert_dt_cols(processing_movement, date_cols)

    return processing_movement


def process_ft_daily_pdt_processing_movement_ts(processing_movement):

    date_cols = get_date_cols(processing_movement)
    processing_movement = convert_dt_cols(processing_movement, date_cols)

    return processing_movement


def process_current_inventory_report(inv, file_name):

    with pd.ExcelWriter(f'{file_name}.xlsx', engine='openpyxl') as writer:

        for pdt_cat in inv['pdt_main_cat'].unique():

            output_sheet = inv[inv['pdt_main_cat'] == pdt_cat]
            output_sheet = output_sheet.sort_values(
                by='available_inv', ascending=False)
            output_sheet.to_excel(writer, sheet_name=pdt_cat, index=False)

            worksheet = writer.sheets[pdt_cat]
            for column in worksheet.columns:
                auto_adjust_column(worksheet, column)
                add_borders_to_column(column)
                if "date" in column[0].value:
                    format_column_to_date(column)


def process_sales_pricing_report(inv, pdt_industry_pc, file_name):

    industries = pdt_industry_pc['industry'].unique()

    with pd.ExcelWriter(f'{file_name}.xlsx', engine='openpyxl') as writer:

        for pdt_cat in inv['pdt_main_cat'].unique():

            output_sheet = inv[inv['pdt_main_cat'] == pdt_cat]
            num_rows = output_sheet.shape[0]

            inv_columns = list(inv.columns)
            variable_columns = ['min_sales_price', 'per_latest_purchase_cost_min_pc1',
                                'max_sales_price', 'per_latest_purchase_cost_max_pc1']
            columns_to_append = variable_columns * len(industries)
            final_columns = inv_columns + columns_to_append

            for industry in industries:

                tmp_pc1 = pdt_industry_pc[pdt_industry_pc['industry'] == industry].copy(
                )
                tmp_pc1.drop(columns=['industry'], inplace=True)
                tmp_variable_cols = [
                    f'{col}_{industry}' for col in variable_columns]
                tmp_col_names = ['pdt_code'] + tmp_variable_cols
                tmp_pc1.columns = tmp_col_names

                output_sheet = output_sheet.merge(
                    tmp_pc1, on='pdt_code', how='left')

                for tmp_variable_col in tmp_col_names:
                    output_sheet[tmp_variable_col] = output_sheet[tmp_variable_col].fillna(
                        0)

            output_sheet.columns = final_columns
            output_sheet = output_sheet.sort_values(
                by='available_inv', ascending=False)
            output_sheet.to_excel(writer, sheet_name=pdt_cat,
                                  index=False, header=True, startrow=1)

            worksheet = writer.sheets[pdt_cat]
            num_of_static_cols = len(inv_columns)
            num_of_variable_cols = len(variable_columns)

            for ids_num, industry in enumerate(industries):

                starting_col = num_of_static_cols + 1 + ids_num * num_of_variable_cols
                ending_col = starting_col + num_of_variable_cols - 1

                worksheet.cell(row=1, column=starting_col).value = industry
                worksheet.merge_cells(
                    start_row=1, start_column=starting_col, end_row=1, end_column=ending_col)

            for column in worksheet.columns:

                auto_adjust_column(worksheet, column, header_row=1)
                if "price" in column[1].value:
                    format_column_to_currency(column)
                if "date" in column[1].value:
                    format_column_to_date(column)
                if "pc1" in column[1].value:
                    column_letter = column[1].column_letter
                    cell_range = f'{column_letter}3:{
                        column_letter}{num_rows+3}'
                    conditional_formatting_redfill(
                        worksheet, cell_range, 'lessThan', [0.1])
                    conditional_formatting_greenfill(
                        worksheet, cell_range, 'greaterThanOrEqual', [0.1])
                    format_column_to_percentage(column)
                add_borders_to_column(column, num_headers=2)


def process_ft_pdt_monthly_qty_ts(pdt_monthly_qty):

    date_cols = get_date_cols(pdt_monthly_qty)
    pdt_monthly_qty = convert_dt_cols(pdt_monthly_qty, date_cols)

    pdt_monthly_qty = pdt_monthly_qty.set_index('agg_date')
    pdt_monthly_qty = pdt_monthly_qty.groupby(
        ['pdt_code', 'qty_type']).resample(rule='MS').sum()
    pdt_monthly_qty.drop(columns=['pdt_code', 'qty_type'], inplace=True)
    pdt_monthly_qty.reset_index(inplace=True)

    return pdt_monthly_qty

def process_ft_daily_pdt_tracking_pdt_inv_value_ts(daily_pdt_inv_value, pdt_level_metrics,start_date_str, end_date_str):
    
    daily_pdt_inv_value['as_of_date'] = pd.to_datetime(daily_pdt_inv_value['as_of_date'])
    output_collate = []
    
    date_range = pd.date_range(start_date_str, end_date_str)
    
    for pdt_code in daily_pdt_inv_value['pdt_code'].unique():
        
        tmp_daily_inv_value = daily_pdt_inv_value.loc[daily_pdt_inv_value['pdt_code'] == pdt_code, :]
        tmp_daily_inv_value.sort_values(by='as_of_date',inplace=True)
        tmp_daily_inv_value.set_index('as_of_date', inplace=True)       
        tmp_daily_inv_value = tmp_daily_inv_value.resample(
                'D', origin='start_day').sum().reindex(date_range).fillna(0)
        tmp_daily_inv_value['previous_inv_value'] = tmp_daily_inv_value['inv_value'].shift(periods=1)
        tmp_daily_inv_value['inv_value_change'] = tmp_daily_inv_value['inv_value'] - tmp_daily_inv_value['previous_inv_value']
        tmp_daily_inv_value['inv_value_change'] = tmp_daily_inv_value['inv_value_change'].fillna(0)
        
        tmp_daily_inv_value.loc[tmp_daily_inv_value['inv_value_change'] > 0, 'daily_pdt_label'] = 'INCREASE'
        tmp_daily_inv_value.loc[tmp_daily_inv_value['inv_value_change'] == 0, 'daily_pdt_label'] = 'NO CHANGE'
        tmp_daily_inv_value.loc[tmp_daily_inv_value['inv_value_change'] < 0, 'daily_pdt_label'] = 'DECREASE'
        tmp_daily_inv_value.loc[tmp_daily_inv_value['inv_value'] < 0, 'daily_pdt_label'] = 'NEGATIVE'
        tmp_daily_inv_value.drop(columns=['inv_value_change','previous_inv_value', 'pdt_code'], inplace=True)
        tmp_daily_inv_value.reset_index(inplace=True,names='as_of_date')
        tmp_daily_inv_value['pdt_code'] = pdt_code
        
        output_collate.append(tmp_daily_inv_value)
        
    daily_pdt_inv_value_ts = pd.concat(output_collate, ignore_index=True)
    daily_pdt_inv_value_ts = daily_pdt_inv_value_ts.merge(pdt_level_metrics, on='pdt_code',how='left')

    return daily_pdt_inv_value_ts
    

def process_ft_daily_agg_values_ts(daily_agg_values, daily_inv_value,start_date_str, end_date_str):

    daily_agg_values = pd.concat([daily_agg_values,daily_inv_value],ignore_index=True)
    
    category_dict = daily_agg_values[['value_sub_category','value_category']].drop_duplicates().to_dict(orient='list')
    category_mapping = dict(zip(category_dict['value_sub_category'],category_dict['value_category']))
    
    daily_agg_values['as_of_date'] = pd.to_datetime(daily_agg_values['as_of_date'])
    output_collate = []
    
    date_range = pd.date_range(start_date_str, end_date_str)
        
    for value_sub_category in daily_agg_values['value_sub_category'].unique():
        
        value_category = category_mapping[value_sub_category]
        
        tmp_daily_agg_values = daily_agg_values.loc[daily_agg_values['value_sub_category'] == value_sub_category, :]
        tmp_daily_agg_values.sort_values(by='as_of_date',inplace=True)
        tmp_daily_agg_values.set_index('as_of_date', inplace=True)       
        tmp_daily_agg_values = tmp_daily_agg_values.resample(
                'D', origin='start_day').sum().reindex(date_range).fillna(0)
        tmp_daily_agg_values['previous_value'] = tmp_daily_agg_values['value'].shift(periods=1)
        tmp_daily_agg_values['value_change'] = tmp_daily_agg_values['value'] - tmp_daily_agg_values['previous_value']
        tmp_daily_agg_values['value_change'] = tmp_daily_agg_values['value_change'].fillna(0)
        
        tmp_daily_agg_values.loc[tmp_daily_agg_values['value_change'] > 0, 'daily_value_label'] = 'INCREASE'
        tmp_daily_agg_values.loc[tmp_daily_agg_values['value_change'] == 0, 'daily_value_label'] = 'NO CHANGE'
        tmp_daily_agg_values.loc[tmp_daily_agg_values['value_change'] < 0, 'daily_value_label'] = 'DECREASE'
        tmp_daily_agg_values.loc[tmp_daily_agg_values['value'] < 0, 'daily_value_label'] = 'NEGATIVE'
        tmp_daily_agg_values.drop(columns=['value_change','previous_value', 'value_category', 'value_sub_category'], inplace=True)
        tmp_daily_agg_values.reset_index(inplace=True,names='as_of_date')
        tmp_daily_agg_values['value_sub_category'] = value_sub_category
        tmp_daily_agg_values['value_category'] = value_category
        
        output_collate.append(tmp_daily_agg_values)
        
    daily_agg_values_ts = pd.concat(output_collate, ignore_index=True)

    return daily_agg_values_ts
        
def process_ft_sales_agent_performance_ts(monthly_agg_sales, customers):
    
    date_cols = get_date_cols(monthly_agg_sales)
    monthly_agg_sales = convert_dt_cols(monthly_agg_sales, date_cols)
    
    customer_groups = customers.loc[: , ['customer_group_name','payment_terms']].copy()
    customer_groups.drop_duplicates(inplace=True)
    customer_groups = customer_groups.groupby(['customer_group_name'])['payment_terms'].apply(lambda x: '/'.join(x)).reset_index()
    monthly_agg_sales = monthly_agg_sales.merge(customer_groups, on='customer_group_name', how='left')
    
    return monthly_agg_sales
    
def process_ft_customer_group_top_pdts(sales, purchase_prices, pdts, start_date_str, end_date_str):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    purchase_prices_date_cols = get_date_cols(purchase_prices)
    purchase_prices = convert_dt_cols(
        purchase_prices, purchase_prices_date_cols)

    purchase_prices.rename(
        columns={'weighted_price': 'purchase_price'}, inplace=True)
    purchase_prices.drop(columns=['previous_price'], inplace=True)

    pdts = pdts[['pdt_code', 'processed_pdt_ind']]

    sales_req_cols = ['doc_date', 'sales_employee_code', 'sales_employee',
                      'customer_group_name', 'pdt_code', 'pdt_name', 'qty', 'price']
    sales = sales[sales_req_cols]
    sales = sales[sales['price'] > 0.01]
    sales = sales[~sales['pdt_code'].str.startswith('ZS')]
    sales.rename(columns={'doc_date': 'as_of_date'}, inplace=True)

    profit = sales.merge(purchase_prices, on=[
                         'as_of_date', 'pdt_code'], how='left')

    profit['profit_per_qty'] = profit['price'] - profit['purchase_price']
    profit['profit'] = profit['profit_per_qty'] * profit['qty']
    profit['pc1_margin'] = profit['profit_per_qty'] / profit['price']
    profit['revenue'] = profit['price'] * profit['qty']

    groupby_cols = ['sales_employee_code', 'sales_employee',
                    'customer_group_name', 'pdt_code', 'pdt_name']

    total_profit = profit.groupby(groupby_cols)['profit'].sum(
    ).reset_index().rename(columns={"profit": "total_profit"})

    total_revenue = profit.groupby(groupby_cols)['revenue'].sum(
    ).reset_index().rename(columns={"revenue": "total_revenue"})

    total_qty = profit.groupby(groupby_cols)['qty'].sum(
    ).reset_index().rename(columns={"qty": "total_qty"})

    avg_pc1_margin = profit.groupby(groupby_cols)['pc1_margin'].mean(
    ).reset_index().rename(columns={"pc1_margin": "avg_pc1_margin"})

    median_pc1_margin = profit.groupby(groupby_cols)['pc1_margin'].median(
    ).reset_index().rename(columns={"pc1_margin": "median_pc1_margin"})

    customer_group_top_pdts = total_profit.merge(total_revenue, how='left', on=groupby_cols).merge(total_qty, how='left', on=groupby_cols).merge(
        avg_pc1_margin, how='left', on=groupby_cols).merge(median_pc1_margin, how='left', on=groupby_cols).merge(pdts, on=['pdt_code'], how='left')

    customer_group_top_pdts['sample_start_date'] = start_date_str
    customer_group_top_pdts['sample_end_date'] = end_date_str

    numeric_cols = ['total_profit', 'total_revenue',
                    'total_qty', 'avg_pc1_margin', 'median_pc1_margin']

    for col in numeric_cols:
        customer_group_top_pdts[col] = customer_group_top_pdts[col].fillna(0)

    return customer_group_top_pdts