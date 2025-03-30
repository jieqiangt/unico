import pandas as pd
import xlsxwriter
from datetime import datetime


def main():
    df = pd.read_excel('logistics_listing.xlsx', sheet_name='main',
                       header=1, usecols='B:H', engine='openpyxl')
    df.columns = [column.strip() for column in df.columns]

    vehicle_data_collect = {}
    vehicle_max_count = 0
    vehicle_total_count = 0

    driver_data_collect = {}
    driver_max_count = 0
    driver_total_count = 0

    for company in df['Company'].unique():

        vehicle_df = df[df['Company'] == company][[
            'CarPlate Number', 'Vehicle Type']]
        vehicle_count = vehicle_df.shape[0]
        vehicle_data = vehicle_df.to_dict('list')
        vehicle_data_collect[company] = {
            "data": vehicle_data, "count": vehicle_count}
        vehicle_total_count += vehicle_count
        if vehicle_max_count < vehicle_count:
            vehicle_max_count = vehicle_count

        driver_df = df[df['Company'] == company][[
            'Driver']]
        driver_df.dropna(inplace=True)
        driver_count = driver_df.shape[0]
        driver_data = driver_df.to_dict('list')
        driver_data_collect[company] = {
            "data": driver_data, "count": driver_count}
        driver_total_count += driver_count
        if driver_max_count < driver_count:
            driver_max_count = driver_count

    today_str = datetime.today().strftime('%Y-%m-%d')
    workbook = xlsxwriter.Workbook(f'{today_str}_logistics_report.xlsx')

    bold_format = workbook.add_format(
        {
            "bold": 1,
            "align": "center",
            "valign": "vcenter",
        }
    )

    text_format = workbook.add_format(
        {
            "align": "center",
            "valign": "vcenter",
        }
    )

    vehicle_worksheet = workbook.add_worksheet('Vehicle Report')

    row = 0
    col = 1

    vehicle_worksheet.merge_range(0, 0, 1, 0, 'No.', bold_format)

    for num in range(1, vehicle_max_count+1):
        vehicle_worksheet.write(num+1, 0, num, bold_format)

    vehicle_worksheet.write(vehicle_max_count+2, 0, 'Total', bold_format)
    vehicle_worksheet.write(vehicle_max_count+3, 0, 'Grand Total', bold_format)
    vehicle_worksheet.write(vehicle_max_count+3, 1,
                            vehicle_total_count, bold_format)

    for company in vehicle_data_collect:
        vehicle_worksheet.merge_range(0, col, 0, col + 1, company, bold_format)
        company_data = vehicle_data_collect[company]["data"]

        for idx, col_name in enumerate(company_data):
            vehicle_worksheet.write(1, col + idx, col_name, text_format)
            vehicle_worksheet.write_column(
                2, col + idx, company_data[col_name], text_format)

        vehicle_worksheet.merge_range(vehicle_max_count+2, col, vehicle_max_count+2,
                                      col + 1, vehicle_data_collect[company]["count"], bold_format)
        col += 2

    vehicle_worksheet.autofit()

    driver_worksheet = workbook.add_worksheet('Drivers Report')

    row = 0
    col = 1

    driver_worksheet.merge_range(0, 0, 1, 0, 'No.', bold_format)

    for num in range(1, driver_max_count+1):
        driver_worksheet.write(num+1, 0, num, bold_format)

    driver_worksheet.write(driver_max_count+2, 0, 'Total', bold_format)
    driver_worksheet.write(driver_max_count+3, 0, 'Grand Total', bold_format)
    driver_worksheet.write(driver_max_count+3, 1,
                           driver_total_count, bold_format)

    for company in driver_data_collect:
        driver_worksheet.merge_range(0, col, 0, col + 1, company, bold_format)
        company_data = driver_data_collect[company]["data"]

        for idx, col_name in enumerate(company_data):
            driver_worksheet.write(1, col + idx, col_name, text_format)
            driver_worksheet.write_column(
                2, col + idx, company_data[col_name], text_format)

        driver_worksheet.merge_range(driver_max_count+2, col, driver_max_count+2,
                                     col + 1, driver_data_collect[company]["count"], bold_format)
        col += 2

    driver_worksheet.autofit()

    workbook.close()


if __name__ == "__main__":
    main()
