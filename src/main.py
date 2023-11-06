from scripts.initialize_db import init_dim_customers, init_dim_suppliers, init_ft_cashflow_monthly_ts, init_ft_cashflow_monthly_by_type_ts, init_ft_suppliers_monthly_pv_ts, init_ft_current_inventory, init_ft_sales_agent_performance_ts, init_ft_recent_sales, init_ft_recent_purchases, init_ft_pdt_monthly_summary_ts, init_ft_recent_ar_invoices, init_ft_recent_ap_invoices
from scripts.update_db import update_ft_current_inventory, update_ft_cashflow_monthly_by_type_ts, update_ft_cashflow_monthly_ts, update_ft_suppliers_monthly_pv_ts, update_ft_recent_purchases, update_ft_sales_agent_performance_ts, update_ft_recent_sales, update_ft_pdt_monthly_summary_ts, update_ft_recent_ar_invoices, update_ft_recent_ap_invoices
from scripts.create_reports import create_ft_sales_ops_report, create_ft_procurement_ops_report, create_ft_pdt_summary, create_ft_sales_orders_alerts, create_ft_pdt_loss_summary,  test_logging, create_ft_purchases_alerts, create_int_pdt_purchase_price_ts, create_ft_current_inv_value, create_ft_current_account_balances, create_ft_accounts_aging_ts, create_ft_outstanding_ar_breakdown, create_ft_outstanding_ap_breakdown


# init_dim_suppliers('2021-01-01')
# init_dim_customers('2021-01-01')

# init_ft_recent_sales()
# init_ft_cashflow_monthly_ts()
# init_ft_cashflow_monthly_by_type_ts()
# init_ft_suppliers_monthly_pv_ts()
# init_ft_sales_agent_performance_ts()
# init_ft_recent_purchases()
# init_ft_current_inventory()
# init_ft_pdt_monthly_summary_ts()
# init_ft_recent_ar_invoices()
# init_ft_recent_ap_invoices()

test_logging()
create_int_pdt_purchase_price_ts()
create_ft_pdt_summary()
update_ft_current_inventory()
create_ft_sales_ops_report()
create_ft_procurement_ops_report()
create_ft_sales_orders_alerts()
create_ft_purchases_alerts()
create_ft_pdt_loss_summary()
create_ft_current_inv_value()
create_ft_current_account_balances()
create_ft_accounts_aging_ts()
create_ft_outstanding_ar_breakdown()
create_ft_outstanding_ap_breakdown()


update_ft_cashflow_monthly_ts()
update_ft_cashflow_monthly_by_type_ts()
update_ft_suppliers_monthly_pv_ts()
update_ft_sales_agent_performance_ts()
update_ft_pdt_monthly_summary_ts()
update_ft_recent_sales()
update_ft_recent_purchases()
update_ft_recent_ar_invoices()
update_ft_recent_ap_invoices()

test_logging()


