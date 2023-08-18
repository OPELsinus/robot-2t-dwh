# import os
# import shutil
#
#
#
# from config import process_list_path
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

a = '18.07.2023'

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# from openpyxl import load_workbook, Workbook
# import pandas as pd
#
# months = [
#     'Январь',
#     'Февраль',
#     'Март',
#     'Апрель',
#     'Май',
#     'Июнь',
#     'Июль',
#     'Август',
#     'Сентябрь',
#     'Октябрь',
#     'Ноябрь',
#     'Декабрь'
# ]
#
# dick = dict()
#
# for file in os.listdir(r'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\1583'):
#     code = file.split('_')[-1].split('.')[0]
#
#     book = load_workbook(fr'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\1583\{file}')
#
#     sheet = book.active
#
#     value = round(float(sheet[f'H12'].value) / 1000)
#
#     dick.update({code: [str(sheet[f'C11'].value)]})
#     dick.get(code).append(value)
#     dick.get(code).append(round(value - (value * 15 / 100)))
#
#     book.close()
# print(dick)
# print(dick.keys())
#
#
# for file in os.listdir(r'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\290'):
#     code = file.split('_')[-1].split('.')[0]
#     if code in dick.keys():
#         df = pd.read_excel(fr'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\290\{file}')
#
#         value = round((sum(df['Товарный остаток на конец, тг']) - sum(df['Сумма проблемного прихода'])) / 1000)
#
#         dick.get(code).append(value)
# # print(list(dick.keys())[3])
# book = Workbook()
#
# sheet = book.active
#
# end_date = '31.08.2023'
# month = int(end_date.split('.')[1])
#
# letters = 'DEFGHIJKLMNO'
#
# for ind, letter in enumerate(letters):
#
#     sheet[f'{letter}1'].value = months[ind]
#
# start_row = 0
# rows_to_merge = 3
#
# for i in range(len(dick)):
#
#     key = list(dick.keys())[i]
#     values = dick.get(key)
#
#     sheet[f'A{start_row + 2}'].value = int(key)
#     sheet.merge_cells(f'A{start_row + 2}:A{start_row + 4}')
#
#     sheet[f'B{start_row + 2}'].value = values[0]
#     sheet.merge_cells(f'B{start_row + 2}:B{start_row + 4}')
#
#     sheet[f'C{start_row + 2}'].value = 'Всего'
#     sheet[f'C{start_row + 3}'].value = 'Из них продовольственные товары'
#     sheet[f'C{start_row + 4}'].value = 'Товарные запасы на конец отчетного месяца'
#
#     sheet[f'{letters[month - 1]}{start_row + 2}'].value = values[1]
#     sheet[f'{letters[month - 1]}{start_row + 3}'].value = values[2]
#     sheet[f'{letters[month - 1]}{start_row + 4}'].value = values[3]
#
#     start_row += rows_to_merge
#
# sheet.column_dimensions['B'].width = 44
#
# book.save('loooolus.xlsx')
# book.close()
# # print(c)
# # print(c1)
# #
# # for i in dick.keys():
# #     print(i, dick.get(i))
# #
# # print(len(dick))
#
#
#
