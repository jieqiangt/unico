
import pandas as pd
import numpy as np
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


def calculate_pdt_summary(df, sampled_months, data_prefix, base=None):

    groupby_cols = ['pdt_code', 'pdt_name']

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

    # getting num unique bp
    bp_map = {'sales': 'customer', 'purchases': 'supplier'}
    req_cols = ['pdt_code', 'pdt_name', f'{bp_map[data_prefix]}_code']
    num_bp = df[req_cols].drop_duplicates().groupby(
        ['pdt_code', 'pdt_name'])[f'{bp_map[data_prefix]}_code'].nunique().reset_index()
    num_bp.rename(columns={
                  f'{bp_map[data_prefix]}_code': f'num_{bp_map[data_prefix]}'}, inplace=True)

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

    total_qty[f'summary_{data_prefix}_avg_qty_per_month'] = total_qty[f'summary_{data_prefix}_total_qty'] / sampled_months

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
            dfs=[latest_date, latest_price, price_summary, total_qty, total_value, num_bp], merge_keys=['pdt_code', 'pdt_name'])
        return summary

    summary = merge_all_df(
        dfs=[base, latest_price, latest_date, price_summary, total_qty, total_value, num_bp], merge_keys=['pdt_code', 'pdt_name'])

    return summary


def process_ft_pdt_summary(sales, purchases, products, purchase_prices, query_start_date_str, sample_start_date_str, end_date_str, sampled_months):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    purchases_date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, purchases_date_cols)

    purchase_prices['as_of_date'] = pd.to_datetime(
        purchase_prices['as_of_date'])
    purchase_prices.drop(columns=['previous_price'],inplace=True)
    purchase_prices.rename(columns={'weighted_price':'purchase_price'},inplace=True)
    
    # filtering pdt giveaways
    sales = sales[sales['price'] > 0.01]

    # calculating sales summary
    base_sales = sales[['pdt_code', 'pdt_name']].drop_duplicates()
    sampled_sales = sales.loc[sales['doc_date'] >= sample_start_date_str]

    # for pdts having sales within sampled timeframe
    sales_sampled_summary = calculate_pdt_summary(
        sampled_sales,  sampled_months, 'sales', base_sales)
    sales_within_sample_summary = sales_sampled_summary[~sales_sampled_summary['summary_sales_latest_date'].isna(
    )]
    sales_within_sample_summary['summary_sales_start_date'] = sample_start_date_str
    sales_within_sample_summary['summary_sales_end_date'] = end_date_str

    # for pdts having sales outside sampled timeframe
    sales_outside_sample = sales[sales['pdt_code'].isin(
        sales_sampled_summary.loc[sales_sampled_summary['summary_sales_latest_date'].isna(), 'pdt_code'])]

    if not sales_outside_sample.empty:
        sales_outside_sample_summary = calculate_pdt_summary(
            sales_outside_sample, sampled_months, 'sales')
        sales_outside_sample_summary['summary_sales_start_date'] = query_start_date_str
        sales_outside_sample_summary['summary_sales_end_date'] = end_date_str

        sales_summary = pd.concat(
            [sales_within_sample_summary, sales_outside_sample_summary], ignore_index=True)
    else:
        sales_summary = sales_within_sample_summary.copy()

    # calculating purchases summary
    base_purchases = purchases[['pdt_code', 'pdt_name']].drop_duplicates()
    sampled_purchases = purchases.loc[purchases['doc_date']
                                      >= sample_start_date_str]

    # for pdts having purchases within sampled timeframe
    purchases_sampled_summary = calculate_pdt_summary(
        sampled_purchases, sampled_months, 'purchases', base_purchases)
    purchases_within_sample_summary = purchases_sampled_summary[
        ~purchases_sampled_summary['summary_purchases_latest_date'].isna()]
    purchases_within_sample_summary['summary_purchases_start_date'] = sample_start_date_str
    purchases_within_sample_summary['summary_purchases_end_date'] = end_date_str

    # for pdts having purchases outside sampled timeframe
    purchases_outside_sample = purchases[purchases['pdt_code'].isin(
        purchases_sampled_summary.loc[purchases_sampled_summary['summary_purchases_latest_date'].isna(), 'pdt_code'])]

    if not purchases_outside_sample.empty:
        purchases_outside_sample_summary = calculate_pdt_summary(
            purchases_outside_sample, sampled_months, 'purchases')
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

    pdt_summary = sales_summary.merge(products, how='left', on=['pdt_code']).merge(
        purchases_summary, how='left', on=['pdt_code', 'pdt_name']).merge(
            pdt_profit, how='left', on=['pdt_code']).merge(
                num_loss_orders, how='left', on=['pdt_code']).merge(
                    total_losses, how='left', on=['pdt_code'])

    return pdt_summary


def process_ft_sales_orders_alerts(current_sales, pdt_summary, purchase_prices):
    
    date_cols = get_date_cols(current_sales)
    current_sales = convert_dt_cols(current_sales, date_cols)
    
    current_sales = current_sales[current_sales['price'] > 0.01]
    purchase_prices['as_of_date'] = pd.to_datetime(
        purchase_prices['as_of_date'])

    sales_orders_alerts = current_sales.merge(
        pdt_summary, on=['pdt_code', 'pdt_name'], how='left').merge(
            purchase_prices, left_on=['doc_date', 'pdt_code'], right_on=['as_of_date', 'pdt_code'], how='left')
    
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


def process_sales_ops_report(pdt_base, inv):

    agg_inv = inv.groupby(['pdt_code'])[
        'on_hand'].sum().reset_index()
    agg_inv.rename(
        columns={"on_hand": "current_inv"}, inplace=True)

    report = pdt_base.merge(agg_inv, on=['pdt_code'], how="left")

    renamed_columns = {'summary_purchases_date_range': 'purchases_data_date_range',
                       'summary_purchases_min_price': 'purchases_min_price',
                       'summary_purchases_max_price': 'purchases_max_price',
                       'summary_sales_date_range': 'sales_data_date_range',
                       'summary_sales_latest_date': 'latest_sales_date',
                       'summary_sales_latest_price': 'latest_sales_price',
                       'summary_sales_min_price': 'sales_min_price',
                       'summary_sales_max_price': 'sales_max_price'}
    report.rename(columns=renamed_columns, inplace=True)

    cols_seq = ['pdt_code',
                'pdt_name',
                'current_inv',
                'purchases_data_date_range',
                'purchases_min_price',
                'purchases_max_price',
                'sales_data_date_range',
                'latest_sales_date',
                'latest_sales_price',
                'sales_min_price',
                'sales_max_price'
                ]

    return report[cols_seq]


def process_procurement_ops_report(pdt_base, inv):

    agg_inv = inv.groupby(['pdt_code'])[
        'on_hand'].sum().reset_index()
    agg_inv.rename(
        columns={"on_hand": "current_inv"}, inplace=True)

    report = pdt_base.merge(agg_inv, on=['pdt_code'], how="left")

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


def process_ft_sales_agent_performance_ts(sales, credit_notes):

    sales_req_cols = ['doc_date', 'sales_employee_code',
                      'sales_employee', 'amount']
    sales_date_cols = get_date_cols(sales[sales_req_cols])
    sales = convert_dt_cols(sales[sales_req_cols], sales_date_cols)
    groupby_cols = [pd.Grouper(
        freq='MS'), 'sales_employee_code', 'sales_employee']
    num_cols = ["amount"]

    sales_ts_index = sales.set_index('doc_date')
    grouper_sales = sales_ts_index.groupby(groupby_cols)
    sales_agent_ts = grouper_sales[num_cols].sum().reset_index()

    sales_agent_ts.rename(
        columns={'doc_date': 'agg_date', 'amount': 'sales_amount'}, inplace=True)

    credit_notes_date_cols = get_date_cols(credit_notes)
    credit_notes = convert_dt_cols(
        credit_notes, credit_notes_date_cols)

    credit_notes_ts_index = credit_notes.set_index('doc_date')
    grouper_credit_notes = credit_notes_ts_index.groupby(groupby_cols)
    credit_notes_ts = grouper_credit_notes[num_cols].sum().reset_index()

    credit_notes_ts.rename(
        columns={'doc_date': 'agg_date', 'amount': 'credit_notes_amount'}, inplace=True)

    sales_agent_performance_ts = sales_agent_ts.merge(credit_notes_ts, on=[
                                                      'agg_date', 'sales_employee_code', 'sales_employee'], how='left')
    sales_agent_performance_ts['sales_amount'] = sales_agent_performance_ts['sales_amount'].fillna(
        0)
    sales_agent_performance_ts['credit_notes_amount'] = sales_agent_performance_ts['credit_notes_amount'].fillna(
        0)

    sales_agent_performance_ts['sales_amount'] = sales_agent_performance_ts['sales_amount'] - \
        sales_agent_performance_ts['credit_notes_amount']

    sales_agent_performance_ts.drop(
        columns=['credit_notes_amount'], inplace=True)

    return sales_agent_performance_ts


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

    return sales_with_purchase_price


def process_ft_recent_purchases(purchases):

    date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, date_cols)

    purchases["agg_date"] = purchases['doc_date'].dt.to_period(
        'M').dt.to_timestamp()

    return purchases


def process_ft_pdt_monthly_summary_ts(sales, purchases):

    sales_date_cols = get_date_cols(sales)
    sales = convert_dt_cols(sales, sales_date_cols)
    purchases_date_cols = get_date_cols(purchases)
    purchases = convert_dt_cols(purchases, purchases_date_cols)

    sales = sales[sales['price'] > 0.01]
    sales["agg_date"] = sales['doc_date'].dt.to_period('M').dt.to_timestamp()
    purchases["agg_date"] = purchases['doc_date'].dt.to_period(
        'M').dt.to_timestamp()

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

    pdt_monthly_summary_ts = pd.concat(
        [sales_summary_ts, purchases_summary_ts], ignore_index=True)

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
        calculated_weighted_prices.rename(columns={'index': 'as_of_date'},inplace=True)
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
    profit['pc1_margin'] = profit['profit_loss_per_qty'] / profit['price']
    losses = profit[profit['profit_loss'] <= 0]

    total_profit = profit.groupby('pdt_code')['profit_loss'].sum(
    ).reset_index().rename(columns={"profit_loss": "total_profit_loss"})

    median_pc1_margin = profit.groupby('pdt_code')['pc1_margin'].mean(
    ).reset_index().rename(columns={"pc1_margin": "avg_pc1_margin"})

    num_losses = losses.groupby('pdt_code')['profit_loss'].count(
    ).reset_index().rename(columns={"profit_loss": "num_losses"})

    total_losses = losses.groupby('pdt_code')['profit_loss'].sum(
    ).reset_index().rename(columns={"profit_loss": "total_losses"})
    total_losses['total_losses'] = np.abs(total_losses['total_losses'])

    pdt_loss_summary = total_profit.merge(
        median_pc1_margin, how='left', on=['pdt_code']).merge(
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
    