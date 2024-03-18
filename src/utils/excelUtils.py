from openpyxl.styles import Alignment
from openpyxl.styles.borders import Border, Side


def get_column_max_width(column):

    max_length = 0
    
    for cell in column:
        try:
            if len(str(cell.value)) > max_length:
                max_length = len(cell.value)
        except:
            pass
    return (max_length + 2) * 1.2

def auto_adjust_column(worksheet, column):

    adjusted_width = get_column_max_width(column)
    column_letter = column[0].column_letter
    worksheet.column_dimensions[column_letter].width = adjusted_width

def format_column_to_currency(column):
    
    if "price" in column[0].value:
        for cell in column:
            cell.number_format = '$#,##0.00'
            
def add_borders_center_align_cell(cell):
    
    border_fmt = Border(top = Side(style='thin'),
    right = Side(style='thin'),
    bottom = Side(style='thin'),
    left = Side(style='thin'))
    
    cell.border = border_fmt
    cell.alignment = Alignment(horizontal='center')
    
def add_borders_to_column(column):
    
    for cell in column:
        add_borders_center_align_cell(cell)
    
    column[0].style='Pandas'