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


def one_big_excel():
    from openpyxl import load_workbook
    from openpyxl.styles import Alignment, PatternFill, Border

    import os

    from config import working_path

    ind = 11

    cells = 'ABCDEFGHIJKLM'

    workbook, sheet1 = None, None

    for file in os.listdir(os.path.join(working_path, f'1583')):
        print(file)

        if ind == 11:

            workbook = load_workbook(os.path.join(working_path, f'1583\\{file}'))

            sheet1 = workbook.active
            sheet1.unmerge_cells('A12:C12')

        book = load_workbook(os.path.join(working_path, f'1583\\{file}'))
        sheet = book.active

        for i in cells:
            try:
                sheet1[i + str(ind)].value = sheet[i + '11'].value
                sheet1[i + str(ind)].alignment = Alignment(
                    horizontal=sheet[i + '11'].alignment.horizontal,
                    vertical=sheet[i + '11'].alignment.vertical,
                    text_rotation=sheet[i + '11'].alignment.text_rotation,
                    wrap_text=sheet[i + '11'].alignment.wrap_text,
                    shrink_to_fit=sheet[i + '11'].alignment.shrink_to_fit,
                    indent=sheet[i + '11'].alignment.indent,
                )
                sheet1[i + str(ind)].fill = PatternFill(
                    start_color=sheet[i + '11'].fill.start_color,
                    end_color=sheet[i + '11'].fill.end_color,
                    fill_type=sheet[i + '11'].fill.fill_type,
                )
                sheet1[i + str(ind)].border = Border(
                    left=sheet[i + '11'].border.left,
                    right=sheet[i + '11'].border.right,
                    top=sheet[i + '11'].border.top,
                    bottom=sheet[i + '11'].border.bottom,
                )

            except:
                sheet1.unmerge_cells([f'A{ind}:C{ind}'])
                sheet1[i + str(ind)].value = sheet[i + '11'].value

        ind += 1

    if workbook is not None:
        workbook.save('result_a.xlsx')


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
            os.makedirs(os.path.join(working_path, '1583'))
        except:
            pass

        book.save(os.path.join(working_path, f'1583\\{start_date}_{end_date}_1583_{df4["Номер объекта"].iloc[i]}.xlsx'))


def report_290(branches, branch_id, start_date, end_date):
    conn = psycopg2.connect(dbname=db_name, host='172.16.10.22', port='5432',
                            user=db_username, password=db_password)

    cur = conn.cursor(name='290_report')

    query = f"""
        with
        report_290_temp_table 
          as 
            (
              select
               ooi.code_item as opt
               , unnest(string_to_array(oiv.value, ','))::bigint as code_pattern
               , oiv.code_subject as code_shop
             from ods.v_spr_opts_repository_options_sat oro 
               inner join ods.v_spr_opts_option_items_sat ooi on ooi.code_option = oro.code_option and current_date between ooi.open_date and ooi.close_date
               inner join ods.v_opts_item_values_sat oiv on oiv.code_item = ooi.code_item and current_date between oiv.open_date and oiv.close_date
             where
               1=1
               and current_date between oro.open_date and oro.close_date
               and ooi.code_item in ('198','199','7367','249','436','251','437','3523','3524')
               and oiv.code_subject = 401
             ),
        report_290_temp_table2 
          as 
            (
             select
               ooi.code_item as opt
               , unnest(string_to_array(oiv.value, ','))::bigint as code_pattern
               , oiv.code_subject as code_shop
             from 
               ods.v_spr_opts_repository_options_sat oro 
               inner join ods.v_spr_opts_option_items_sat ooi on ooi.code_option = oro.code_option and current_date between ooi.open_date and ooi.close_date
               inner join ods.v_opts_item_values_sat oiv on oiv.code_item = ooi.code_item and current_date between oiv.open_date and oiv.close_date
             where
               1=1
               and current_date between oro.open_date and oro.close_date
               and ooi.code_item in ('200','6371','6370','248','235','413','3525','6494','6493','7385')
               and oiv.code_subject = 401
             )
        SELECT 
               w.number_shop,
               ds1.store_name,
               SUM((coalesce(ww.q_real, 0)-coalesce(ww.q_debet_bc, 0)+coalesce(ww.q_credit_bc, 0)-coalesce(ww.q_debet_ab, 0)+coalesce(ww.q_credit_ab, 0)+coalesce(ww.q_credit_ab_rtl, 0)+coalesce(ww.q_spisper_ab, 0)+coalesce(ww.q_spisper_bc, 0)+coalesce(ww.q_retsup_ab, 0)+coalesce(ww.q_retsup_bc, 0))/au.coefficient) q_begin, --Товарный остаток на начало, кол-во
               ROUND(SUM(coalesce(ww.p_real, 0)-coalesce(ww.p_debet_bc, 0)+coalesce(ww.p_creditbc_prihod, 0)-coalesce(ww.p_debet_ab, 0)+coalesce(ww.p_creditab_prihod, 0)+coalesce(ww.p_creditab_prihod_rtl, 0)+coalesce(ww.p_spisper_bc, 0)+coalesce(ww.p_spisper_ab, 0)+coalesce(ww.p_retsup_bc, 0)+coalesce(ww.p_retsup_ab, 0)), 2) p_begin,--Товарный остаток на начало, тг
               ROUND(SUM((coalesce(ww.q_problem, 0)+coalesce(rtl2.q_credit_ab, 0))/au.coefficient), 3) q_problem,--Кол-во проблемного прихода
               ROUND(coalesce(SUM(ww.p_problem),0)+coalesce(SUM(rtl2.p_credit_ab_prihod),0),2) p_problem,     --Сумма проблемного прихода
               SUM((coalesce(ww.q_real, 0)-coalesce(ww.q_debet_bc, 0)+coalesce(ww.q_credit_bc, 0)+coalesce(ww.q_spisper_bc, 0)+coalesce(ww.q_retsup_bc, 0))/au.coefficient) q_end, --Товарный остаток на конец, кол-во
               ROUND(SUM(coalesce(ww.p_real, 0)-coalesce(ww.p_debet_bc, 0)+coalesce(ww.p_creditbc_prihod, 0)+coalesce(ww.p_spisper_bc, 0)+coalesce(ww.p_retsup_bc, 0)), 2) p_end   --Товарный остаток на конец, тг   
               FROM 
            (SELECT 
               ss.code_shop,ss.number_shop,w.code_wares,w.name_wares,w.code_group 
             FROM 
               ods.v_wares_sat w,
               ods.v_subgroup_shop_sat ss 
             WHERE ss.code_shop IN ({branches})
               and 
               current_date between w.open_date and w.close_date
               and current_date between ss.open_date and ss.close_date
             ) w
            inner join ods.v_addition_unit_sat au on au.code_wares = w.code_wares and current_date between au.open_date and au.close_date 
            inner join ods.v_unit_dimension_sat dau on dau.code_unit = au.code_unit and current_date between dau.open_date and dau.close_date
            left join dwh_data.dim_store ds1 on ds1.store_number = w.number_shop and current_date between ds1.datestart and ds1.dateend
            left join 
            (SELECT wi.code_shop,wi.code_wares, 
                      SUM(wi.quantity_warehouse) q_real, 
                      SUM(wi.quantity_warehouse*wi.price_coming_basis*(1+1*wi.vat)) p_real,
                      coalesce(SUM(pr.q_debet_bc),0) q_debet_bc, 
                      coalesce(SUM(pr.q_debet_ab),0) q_debet_ab,
                      coalesce(SUM(pr.q_debet_bc*wi.price_coming_basis*(1+1*wi.vat)),0) p_debet_bc,
                      coalesce(SUM(pr.q_debet_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_debet_ab, 
                      coalesce(SUM(pr.q_return),0) q_return,
                      coalesce(SUM(pr.q_return*wi.price_by_invoice_basis*(1+1*wi.vat_invoice)),0) p_return_pro,
                      coalesce(SUM(pr.q_return_rtl),0) q_return_rtl,
                      coalesce(SUM(pr.q_return_rtl*wi.price_by_invoice_basis*(1+1*wi.vat_invoice)),0) p_return_rtl_pro,
                      coalesce(SUM(pr.q_problem),0) q_problem,               
                      coalesce(SUM(pr.q_problem*wi.price_coming_basis*(1+1*wi.vat)),0) p_problem,
                      coalesce(SUM(ras.q_credit_bc),0)+coalesce(SUM(rtl.q_credit_bc),0) q_credit_bc, 
                      coalesce(SUM(ras.q_credit_ab),0) q_credit_ab, 
                      coalesce(SUM(rtl.q_credit_ab),0) q_credit_ab_rtl,
                      coalesce(SUM((coalesce(ras.q_credit_bc,0)+coalesce(rtl.q_credit_bc,0))*wi.price_coming_basis*(1+1*wi.vat)),0) p_creditbc_prihod,
                      coalesce(SUM(ras.q_credit_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_credit_ab, 
                      coalesce(SUM(rtl.q_credit_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_credit_ab_rtl, 
                      coalesce(SUM(ras.q_credit_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_creditab_prihod,
                      coalesce(SUM(rtl.q_credit_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_creditab_prihod_rtl, 
                      coalesce(SUM(spisper.q_spisper_bc),0) q_spisper_bc, 
                      coalesce(SUM(spisper.q_spisper_ab),0) q_spisper_ab,
                      coalesce(SUM(spisper.q_spisper_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_spisper_ab,
                      coalesce(SUM(spisper.q_spisper_bc*wi.price_coming_basis*(1+1*wi.vat)),0) p_spisper_bc,              
                      coalesce(SUM(spisper.q_spis),0) q_spis, 
                      coalesce(SUM(spisper.q_spis*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis,
                      coalesce(SUM(spisper.q_pered),0) q_pered, 
                      coalesce(SUM(spisper.q_pered*wi.price_coming_basis*(1+1*wi.vat)),0) p_pered,
                      coalesce(SUM(spisper.q_spis_invent),0) q_spis_invent,
                      coalesce(SUM(spisper.q_spis_invent*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis_invent,
                      coalesce(SUM(spisper.q_spis_invent*wi.price_coming_basis*(1*wi.vat)),0) p_spis_invent_vat,
                      coalesce(SUM(spisper.sum_spis_invent),0) p_spis_invent_sp,
                      coalesce(SUM(spisper.q_spis_prd),0) q_spis_prd,
                      coalesce(SUM(spisper.q_spis_prd*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis_prd,
                      coalesce(SUM(spisper.q_spis_resort),0) q_spis_resort,
                      coalesce(SUM(spisper.q_spis_resort*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis_resort,              
                      coalesce(SUM(spisper.sum_spis_resort),0) p_spis_resort_sp,
                      coalesce(SUM(retsup.q_retsup_bc),0) q_retsup_bc, 
                      coalesce(SUM(retsup.q_retsup_ab),0) q_retsup_ab,
                      coalesce(SUM(retsup.p1_retsup_ab),0) p1_retsup_ab,
                      coalesce(SUM(retsup.q_retsup_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_retsup_ab,
                      coalesce(SUM(retsup.q_retsup_bc*wi.price_coming_basis*(1+1*wi.vat)),0) p_retsup_bc
                 FROM 
                   ods.wares_invoice_link wi
                   left join
                   (
                   SELECT wi.code_wares, wi.code_invoice, 
                             SUM(DECODE(SIGN(wi.date_invoice::date-to_date('{end_date}','dd.mm.yyyy')),1,wi.quantity_in_basis, 0)) q_debet_bc,
                             SUM(DECODE(SIGN(wi.date_invoice::date-to_date('{end_date}','dd.mm.yyyy')),1,0,wi.quantity_in_basis)) q_debet_ab,
                             SUM(DECODE(SIGN(wi.date_invoice::date-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN ir.code_invoice IS NULL THEN DECODE(RTRIM(wi.type_invoice),'V',wi.quantity_in_basis,0) ELSE 0 END)) q_return,
                             SUM(DECODE(SIGN(wi.date_invoice::date-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN ir.code_invoice IS NOT NULL THEN wi.quantity_in_basis ELSE 0 END)) q_return_rtl,
                             SUM(DECODE(SIGN(wi.date_invoice::date-to_date('{end_date}','dd.mm.yyyy')),1,0,DECODE(RTRIM(wi.problem),'Y',wi.quantity_in_basis, 0))) q_problem--,
                             FROM ods.wares_invoice_link wi
                      left join ods.invoice_rtl_order_link ir on wi.code_invoice = ir.code_invoice
                      left join report_290_temp_table opts1 on opts1.opt IN (198,199,7367) AND opts1.code_shop=wi.code_shop AND opts1.code_pattern=wi.code_pattern
                      left join report_290_temp_table opts2 on opts2.opt IN (7367) AND opts2.code_shop=wi.code_shop AND opts2.code_pattern=wi.code_pattern
                      left join report_290_temp_table opts3 on opts3.opt IN (249,436,251,437) AND opts3.code_shop=wi.code_shop AND opts3.code_pattern=wi.code_pattern
                      left join report_290_temp_table opts4 on opts4.opt IN (3523,3524) AND opts4.code_shop=wi.code_shop AND opts4.code_pattern=wi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (198,199,7367) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern)
                    WHERE wi.date_invoice >= to_date('{start_date}','dd.mm.yyyy') 
                      AND wi.state_invoice = 'W'                  
                      AND wi.code_shop IN ({branches})
                    GROUP BY wi.code_wares, wi.code_invoice
                    ) pr on pr.code_wares = wi.code_wares AND pr.code_invoice = wi.code_invoice
                   left join 
                   (SELECT wo.code_wares, wo.code_invoice, 
                             SUM(DECODE(SIGN(wo.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,wo.quantity_in_basis, 0)) q_credit_bc,
                             SUM(DECODE(SIGN(wo.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,wo.quantity_in_basis)) q_credit_ab,
                             SUM(DECODE(SIGN(wo.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,wo.quantity*(1+1*wo.vat)*wo.price)) p_credit_ab
                    FROM ods.wares_out_invoice_link wo
                    WHERE wo.date_from_warehouse >= to_date('{start_date}','dd.mm.yyyy')
                      AND wo.code_shop IN ({branches})
                    GROUP BY wo.code_wares, wo.code_invoice
                    ) ras on ras.code_wares = wi.code_wares AND ras.code_invoice = wi.code_invoice 
                   left join 
                   (SELECT wo.code_wares, wo.code_invoice, 
                             SUM(DECODE(SIGN(wo.date_retail_order-to_date('{end_date}','dd.mm.yyyy')),1,wo.quantity_basis, 0)) q_credit_bc,
                             SUM(DECODE(SIGN(wo.date_retail_order-to_date('{end_date}','dd.mm.yyyy')),1,0,wo.quantity_basis)) q_credit_ab--,
                    FROM ods.rtl_retail_order_write_off_link wo
                    WHERE wo.date_retail_order >= to_date('{start_date}','dd.mm.yyyy')
                      AND wo.code_shop IN ({branches})
                    GROUP BY wo.code_wares, wo.code_invoice
                    ) rtl on rtl.code_wares = wi.code_wares AND rtl.code_invoice = wi.code_invoice
                   left join 
                   (SELECT wwi.code_wares, wwi.code_invoice, 
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,wwi.quantity_in_basis, 0)) q_spisper_bc,                     
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,wwi.quantity_in_basis)) q_spisper_ab,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.variety_write_off_invoice), 'T', 0, wwi.quantity_in_basis))) q_spis,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.variety_write_off_invoice), 'T', 0, wwi.quantity*(1+1*wwi.vat)*wwi.price))) p_spis_sp,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.variety_write_off_invoice), 'T', wwi.quantity_in_basis, 0))) q_pered,                     
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN opts11.opt is not null THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_invent,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN opts12.opt is not null THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_prd,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN opts13.opt is not null THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_resort,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN opts14.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_invent,
                             SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('{end_date}','dd.mm.yyyy')),1,0,CASE WHEN opts16.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_resort
                    FROM ods.wares_write_off_invoice_link wwi
                      left join report_290_temp_table2 opts5 on opts5.opt IN (200,6371,6370,7385) AND opts5.code_shop=wwi.code_shop AND opts5.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts6 on opts6.opt IN (248,235,413) AND opts6.code_shop=wwi.code_shop AND opts6.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts7 on opts7.opt IN (3525,6494,6493) AND opts7.code_shop=wwi.code_shop AND opts7.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1  FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)
                      left join report_290_temp_table2 opts8 on opts8.opt IN (200,6371,6370,7385) AND opts8.code_shop=wwi.code_shop AND opts8.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts9 on opts9.opt IN (248,235,413) AND opts9.code_shop=wwi.code_shop AND opts9.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts10 on opts10.opt IN (3525,6494,6493) AND opts10.code_shop=wwi.code_shop AND opts10.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)
                      left join report_290_temp_table2 opts11 on opts11.opt IN (200,6371,6370,7385) AND opts11.code_shop=wwi.code_shop AND opts11.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts12 on opts12.opt IN (248,235,413) AND opts12.code_shop=wwi.code_shop AND opts12.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts13 on opts13.opt IN (3525,6494,6493) AND opts13.code_shop=wwi.code_shop AND opts13.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1  FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)
                      left join report_290_temp_table2 opts14 on opts14.opt IN (200,6371,6370,7385) AND opts14.code_shop=wwi.code_shop AND opts14.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts15 on opts15.opt IN (248,235,413) AND opts15.code_shop=wwi.code_shop AND opts15.code_pattern=wwi.code_pattern
                      left join report_290_temp_table2 opts16 on opts16.opt IN (3525,6494,6493) AND opts16.code_shop=wwi.code_shop AND opts16.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)
                    WHERE wwi.date_from_warehouse >= to_date('{start_date}','dd.mm.yyyy')
                      AND wwi.code_shop IN ({branches})
                    GROUP BY wwi.code_wares, wwi.code_invoice
                    ) spisper on spisper.code_wares = wi.code_wares AND spisper.code_invoice = wi.code_invoice
                   left join 
                   (SELECT wr.code_wares, wr.code_invoice code_invoice, 
                             SUM(DECODE(SIGN(wr.date_from_warehouse::date-to_date('{end_date}','dd.mm.yyyy')), 1, wr.quantity_in_basis, 0)) q_retsup_bc,
                             SUM(DECODE(SIGN(wr.date_from_warehouse::date-to_date('{end_date}','dd.mm.yyyy')), 1, 0, wr.quantity_in_basis)) q_retsup_ab,
                             SUM(DECODE(SIGN(wr.date_from_warehouse::date-to_date('{end_date}','dd.mm.yyyy')), 1, 0, wr.quantity*(1+1*wr.vat)*wr.price)) p1_retsup_ab
                    FROM ods.wares_in_return_invoice_link wr
                    WHERE wr.date_from_warehouse >= to_date('{start_date}','dd.mm.yyyy')
                      AND wr.code_shop IN ({branches})
                    GROUP BY wr.code_wares, wr.code_invoice
                    ) retsup on retsup.code_wares = wi.code_wares AND retsup.code_invoice = wi.code_invoice
                WHERE wi.state_invoice = 'W' 
                  AND wi.code_shop IN ({branches})
                GROUP BY wi.code_shop,wi.code_wares
              ) ww on ww.code_shop = w.code_shop AND ww.code_wares = w.code_wares
            left join  
              (SELECT wo.code_shop,wo.code_wares, 
                      SUM(wo.quantity_basis) q_credit_ab,
                      SUM(wo.quantity_basis*(1+1*wo.vat)*wo.price_coming_forecast) p_credit_ab_prihod              
               FROM ods.rtl_retail_order_write_off_link wo
               WHERE wo.date_retail_order >= to_date('{start_date}','dd.mm.yyyy')
                 AND wo.date_retail_order <= to_date('{end_date}','dd.mm.yyyy') 
                 AND coalesce(wo.code_invoice, -1) = -1
                 AND wo.code_shop IN ({branches})
               GROUP BY wo.code_shop,wo.code_wares
              ) rtl2 on rtl2.code_shop = w.code_shop AND rtl2.code_wares = w.code_wares 
        WHERE 
          au.default_unit = 'Y'
        GROUP BY w.number_shop, ds1.store_name
        ORDER BY w.number_shop
        """

    cur.execute(query)

    # print('Executed')

    df1 = pd.DataFrame(cur.fetchall())

    cur.close()
    conn.close()

    df1.columns = ['Номер магазина', 'Название магазина', 'Товарный остаток на начало, кол-во', 'Товарный остаток на начало, тг', 'Кол-во проблемного прихода', 'Сумма проблемного прихода', 'Товарный остаток на конец, кол-во', 'Товарный остаток на конец, тг']

    # print(sum(df1['Товарный остаток на конец, тг']))
    # print(sum(df1['Сумма проблемного прихода']))
    # print((sum(df1['Товарный остаток на конец, тг']) - sum(df1['Сумма проблемного прихода'])) / 1000)

    saving_path = os.path.join(working_path, '290')
    df1.to_excel(os.path.join(saving_path, f'{start_date}_{end_date}_{branch_id}.xlsx'), index=False)
    print('Saved')
    return df1


def get_all_branches():
    conn = psycopg2.connect(dbname=db_name, host='172.16.10.22', port='5432',
                            user=db_username, password=db_password)

    cur = conn.cursor()

    query = f"""
            select source_branch_id
            from dwh_data.dim_store
            where current_date between datestart and dateend
            group by source_branch_id
        """

    cur.execute(query)

    print('Executed')

    df1 = pd.DataFrame(cur.fetchall())

    cur.close()
    conn.close()

    df1.columns = ["Номера филиалов"]

    return df1


def get_store_ids_by_branch_id(branch_id):
    conn = psycopg2.connect(dbname=db_name, host='172.16.10.22', port='5432',
                            user=db_username, password=db_password)

    cur = conn.cursor()

    query = f"""
              select source_store_id, sale_source_obj_id, store_name
              from dwh_data.dim_store
              where source_branch_id = {branch_id}
              and current_date between datestart and dateend
              group by source_store_id, sale_source_obj_id, store_name
          """

    cur.execute(query)

    print('Executed')

    df1 = pd.DataFrame(cur.fetchall())

    cur.close()
    conn.close()

    df1.columns = ["Номер магазина", "Код магазина", "Название магазина"]

    return df1


if __name__ == '__main__':

    time_started = time.time()

    today = datetime.date.today()
    first_day_of_current_month = datetime.date(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    first_day_of_previous_month = datetime.date(last_day_of_previous_month.year, last_day_of_previous_month.month, 1)
    last_day_of_preprevious_month = first_day_of_previous_month - datetime.timedelta(days=1)

    start_date = str(last_day_of_preprevious_month)
    end_date = str(last_day_of_previous_month)

    # print(start_date, end_date)
    # ? 1583 report
    df = first_request(start_date, end_date)

    df1 = second_request(start_date, end_date)

    df4 = pd.concat([df1, df], join='inner', axis=1)

    df4.columns = ['Номер филиала', 'Оборот, тг с НДС', 'Оборот, тг без НДС', 'Себестоимость',
                   'Бухгалтерская себестоимость', 'Номер филиала1', 'Номер объекта',
                   'Название филиала', 'Название компании', 'Количество товаров в чеке', 'Выручка, тг с НДС', 'Выручка, тг без НДС', 'Количество чеков']
    df4.insert(1, 'Номер объекта', df4.pop('Номер объекта'))
    df4.insert(2, 'Название филиала', df4.pop('Название филиала'))
    df4.insert(3, 'Название компании', df4.pop('Название компании'))

    df4 = df4.drop(['Номер филиала1'], axis=1)
    print('Saving')
    df4.to_excel('sdfdfsf.xlsx')
    # exit()
    saving_reports(df4, start_date, end_date)

    one_big_excel()
    time_finished = time.time()

    print('Saved successfully')
