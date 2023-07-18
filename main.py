import datetime
import os
import sys
import time

import openpyxl

from config import working_path, db_name, db_username, db_password, chat_id, tg_token

import psycopg2
import pandas as pd

from tools import send_message_to_tg


def first_request(start_date, end_date):

    conn = psycopg2.connect(dbname=db_name, host='172.16.10.22', port='5432',
                            user=db_username, password=db_password)

    cur = conn.cursor(name='1583_first_part')

    query = f"""
        select qfo.source_store_id,
               ds.sale_source_obj_id,
               ds.store_name,
               dss.name_1c,
               sum(qfo.cnt_wares_pos_detail)                                                             as "Кол-во товаров в чеках", -- "Количество товаров в чеке"
               sum(qfo.order_sum_with_vat)                                                               as "Выручка, тг с НДС",
               sum(qfo.order_sum_without_vat)                                                            as "Выручка, тг без НДС",
               sum(qfo.is_sale_order)                                                                    as "Количество чеков"
        from dwh_data.qs_fact_order qfo
        left join dwh_data.dim_store ds on ds.source_store_id = qfo.source_store_id and current_date between ds.datestart and ds.dateend
        left join dwh_data.dim_store_src dss on dss.source_store_id = qfo.source_store_id
        where date(qfo.order_date) between to_date('{start_date}', 'YYYY-MM-DD') and to_date('{end_date}', 'YYYY-MM-DD')
                and qfo."source_store_id"::int > 0 and ds."store_name" like '%Торговый%'
        group by ds.sale_source_obj_id, qfo.source_store_id, ds.store_name, dss.name_1c
             union all
        select   qfog.source_store_id,
                 ds.sale_source_obj_id,
                 ds.store_name,
                 dss.name_1c,
                 sum(qfog.count_wares)                            as quantity,
                 sum(qfog.sum_sale_vat)                           as order_sum_with_vat,
                 sum(qfog.sum_sale_no_vat)                        as order_sum_without_vat,
                 sum(qfog.is_sale_order)                          as sale_order_cnt
        from dwh_data.qs_fact_order_gross qfog
        left join dwh_data.dim_store ds on ds.source_store_id = qfog.source_store_id and current_date between ds.datestart and ds.dateend
        left join dwh_data.dim_store_src dss on dss.source_store_id = qfog.source_store_id
        where date(qfog.order_date) between to_date('{start_date}', 'YYYY-MM-DD') and to_date('{end_date}', 'YYYY-MM-DD')
              and qfog."source_store_id"::int > 0 and ds."store_name" like '%Торговый%'
        group by ds.sale_source_obj_id, qfog.source_store_id, ds.store_name, dss.name_1c
        order by source_store_id;
    """

    cur.execute(query)

    print('Executed')

    df = pd.DataFrame(cur.fetchall())

    cur.close()
    conn.close()

    df.columns = ["Номер филиала", "Номер объекта", "Название филиала", "Название компании", "Количество товаров в чеке", "Выручка, тг с НДС", "Выручка, тг без НДС", "Количество чеков"]

    df3 = df.copy()
    for i in df3['Номер филиала'].unique():
        ids = df3[df3['Номер филиала'] == i].index
        df3.loc[ids[0], df3.columns[4:]] = df3.loc[ids, df3.columns[4:]].sum()
        try:
            df3 = df3.drop([ids[1]])
        except:
            pass

    return df3


def second_request(start_date, end_date):

    conn = psycopg2.connect(dbname=db_name, host='172.16.10.22', port='5432',
                            user=db_username, password=db_password)

    cur = conn.cursor(name='1583_second_part')

    query = f"""
    SELECT
          q.source_store_id::int                                                                    as source_store_id,
          Sum(q."sum_order_wares_vat_nr")                                                           as "Оборот, тг с НДС",
          Sum(q."sum_order_wares_without_vat_nr")                                                   as "Оборот, тг без НДС",
          Sum(q."cost_price_base_t")                                                                as "Себестоимость",
          Sum(q."cost_price_base_t") - Sum(q."cost_price_base_t") *
             (Sum(q."sum_order_wares_vat_nr") - Sum(q."sum_order_wares_without_vat_nr"))
             / decode(Sum(q."sum_order_wares_vat_nr"), 0, null, Sum(q."sum_order_wares_vat_nr"))    as "Бухгалтерская себестоимость"
       FROM "dwh_data"."qs_fact_sales" q
       left join dwh_data.dim_store ds on ds.source_store_id = q.source_store_id and current_date between ds.datestart and ds.dateend
       WHERE date(q.order_date) between to_date('{start_date}', 'YYYY-MM-DD') and to_date('{end_date}', 'YYYY-MM-DD')
        and q."source_store_id"::int > 0  and ds."store_name" like '%Торговый%'
       GROUP BY
          q."source_store_id"
     union all
        SELECT
              sa.source_store_id::int                          as source_store_id,
              Sum("sum_sale")                                  as sum_order_with_vat,
              Sum(sa."sum_sale_no_vat")                        as sum_order_without_vat,
              Sum("sum_coming")                                as cost_price_with_vat ,
              Sum(sa."sum_coming_no_vat")                      as cost_price_without_vat
        FROM "dwh_data"."qs_fact_sales_gross" sa
        left join dwh_data.dim_store ds on ds.source_store_id = sa.source_store_id and current_date between ds.datestart and ds.dateend
        where date(sa.order_date) between to_date('{start_date}', 'YYYY-MM-DD') and to_date('{end_date}', 'YYYY-MM-DD')
       and sa.source_store_id::int > 0  and ds."store_name" like '%Торговый%'
        GROUP BY
            sa.source_store_id
        order by source_store_id;
    """

    cur.execute(query)

    print('Executed')

    df1 = pd.DataFrame(cur.fetchall())

    cur.close()
    conn.close()

    df1.columns = ["Номер филиала", "Оборот, тг с НДС", "Оборот, тг без НДС", "Себестоимость", "Бухгалтерская себестоимость"]

    df2 = df1.copy()
    for i in df2['Номер филиала'].unique():
        ids = df2[df2['Номер филиала'] == i].index
        df2.loc[ids[0], df2.columns[1:]] = df2.loc[ids, df2.columns[1:]].sum()

    df2 = df2.loc[~df2['Номер филиала'].duplicated(keep='first')]

    return df2


def saving_reports(df4, start_date, end_date):
    for i in range(len(df4)):
        book = openpyxl.load_workbook('samples\\2023-04-30_2023-05-31_1583_4.xlsx')
        sheet = book.active

        try:
            start_date = start_date.split('-')[2] + '.' + start_date.split('-')[1] + '.' + start_date.split('-')[0]
            end_date = end_date.split('-')[2] + '.' + end_date.split('-')[1] + '.' + end_date.split('-')[0]
        except:
            ...

        sheet['D2'] = start_date
        sheet['G2'] = end_date
        sheet['B4'].value = str(datetime.datetime.now().strftime("%d.%m.%y %H:%M:%S"))
        sheet['B7'].value = df4['Название филиала'].iloc[i]
        sheet['A11'].value = start_date + '-' + end_date
        sheet['B11'].value = df4['Название филиала'].iloc[i]
        sheet['C11'].value = df4['Название компании'].iloc[i]
        sheet['D11'].value = df4['Количество чеков'].iloc[i]
        sheet['E11'].value = df4['Выручка, тг с НДС'].iloc[i]
        sheet['F11'].value = df4['Выручка, тг без НДС'].iloc[i]
        sheet['G11'].value = df4['Оборот, тг с НДС'].iloc[i]
        sheet['H11'].value = df4['Оборот, тг без НДС'].iloc[i]
        sheet['I11'].value = df4['Себестоимость'].iloc[i]
        sheet['J11'].value = df4['Бухгалтерская себестоимость'].iloc[i]
        sheet['K11'].value = float(df4['Оборот, тг с НДС'].iloc[i]) - float(df4['Себестоимость'].iloc[i])
        try:
            sheet['L11'].value = float(df4['Оборот, тг с НДС'].iloc[i]) / float(df4['Себестоимость'].iloc[i]) - 1
        except:
            sheet['L11'].value = 0
        sheet['L11'].number_format = '0.00%'
        sheet['M11'].value = df4['Количество товаров в чеке'].iloc[i]

        sheet['D12'].value = sheet['D11'].value
        sheet['E12'].value = sheet['E11'].value
        sheet['F12'].value = sheet['F11'].value
        sheet['G12'].value = sheet['G11'].value
        sheet['H12'].value = sheet['H11'].value
        sheet['I12'].value = sheet['I11'].value
        sheet['J12'].value = sheet['J11'].value
        sheet['K12'].value = sheet['K11'].value
        sheet['L12'].value = sheet['L11'].value
        sheet['M12'].value = sheet['M11'].value

        try:
            os.makedirs(os.path.join(working_path, '1583_1'))
        except:
            pass
        book.save(os.path.join(working_path, f'1583_1\\{df4["Название филиала"].iloc[i]}_1583.xlsx'))


if __name__ == '__main__':

    time_started = time.time()

    today = datetime.date.today()
    first_day_of_current_month = datetime.date(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    first_day_of_previous_month = datetime.date(last_day_of_previous_month.year, last_day_of_previous_month.month, 1)
    last_day_of_preprevious_month = first_day_of_previous_month - datetime.timedelta(days=1)

    start_date = str(last_day_of_preprevious_month)
    end_date = str(last_day_of_previous_month)

    df = first_request(start_date, end_date)

    df1 = second_request(start_date, end_date)

    df4 = pd.concat([df1, df], join='inner', axis=1)
    df4.columns = ['Номер филиала', 'Номер объекта', 'Оборот, тг с НДС', 'Оборот, тг без НДС',
                   'Себестоимость', 'Бухгалтерская себестоимость', 'Номер филиала1',
                   'Название филиала', 'Название компании', 'Количество товаров в чеке', 'Выручка, тг с НДС',
                   'Выручка, тг без НДС', 'Количество чеков']
    df4.insert(1, 'Название филиала', df4.pop('Название филиала'))
    df4.insert(2, 'Название компании', df4.pop('Название компании'))
    df4 = df4.drop(['Номер филиала1'], axis=1)

    saving_reports(df4, start_date, end_date)

    time_finished = time.time()

    send_message_to_tg(tg_token, chat_id, f'Выгрузка отчёта 1583 завершилась.\nЗатраченное время: {str(time_finished - time_started)[:6]}c\nКоличесвто филиалов: {len(df4)}')

    print('Saved successfully')
