import xlrd
import os
from datetime import date

def convert_date_from_excel(excel_format_date):
    # tt[1] == month
    # tt[2] == day
    from datetime import datetime
    dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_format_date - 2)
    tt = dt.timetuple()
    return (tt[1], tt[2])

def get_today_date():
    today = date.today()
    return (today.month, today.day)

def get_birthday_date():
    current_date = get_today_date()
    file_path = '/home/rakesh/email/birthday.xlsx'
    # print file_path
    wb = xlrd.open_workbook(file_path)
    sheet_data = wb.sheet_by_index(0)
    send_email_data = []
    for i in range(1, sheet_data.nrows):
        excel_format_date = sheet_data.cell_value(i, 2)
        if excel_format_date:
            actual_birth_date = convert_date_from_excel(int(excel_format_date))
        else:
            # who not provided DOB
            actual_birth_date = (0,0)
        if (actual_birth_date[0] == current_date[0] and \
            actual_birth_date[1] == current_date[1]):
            send_email_data.append(sheet_data.row_values(i))

    print send_email_data
    return send_email_data
if __name__ == '__main__':
    get_birthday_date()