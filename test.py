import psycopg2

from config import db_name, db_username, db_password

conn = psycopg2.connect(dbname=db_name, host='172.16.10.22', port='5432',
                        user=db_username, password=db_password)

cur = conn.cursor(name='1583_second_part')

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
       --and ooi.code_item = 7367
       and oiv.code_subject = 4
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
       --and ooi.code_item = 7367
       and oiv.code_subject = 4
     )
SELECT 
       w.number_shop,
       SUM((coalesce(ww.q_real, 0)-coalesce(ww.q_debet_bc, 0)+coalesce(ww.q_credit_bc, 0)-coalesce(ww.q_debet_ab, 0)+coalesce(ww.q_credit_ab, 0)+coalesce(ww.q_credit_ab_rtl, 0)+coalesce(ww.q_spisper_ab, 0)+coalesce(ww.q_spisper_bc, 0)+coalesce(ww.q_retsup_ab, 0)+coalesce(ww.q_retsup_bc, 0))/au.coefficient) q_begin, 
       ROUND(SUM(coalesce(ww.p_real, 0)-coalesce(ww.p_debet_bc, 0)+coalesce(ww.p_creditbc_prihod, 0)-coalesce(ww.p_debet_ab, 0)+coalesce(ww.p_creditab_prihod, 0)+coalesce(ww.p_creditab_prihod_rtl, 0)+coalesce(ww.p_spisper_bc, 0)+coalesce(ww.p_spisper_ab, 0)+coalesce(ww.p_retsup_bc, 0)+coalesce(ww.p_retsup_ab, 0)), 2) p_begin,
       coalesce(SUM(ww.q_debet_ab/au.coefficient), 0) q_debet_ab,       
       ROUND(coalesce(SUM(ww.p_debet_ab), 0), 2) p_debet_ab,        
       SUM((coalesce(ww.q_debet_ab,0)-coalesce(ww.q_problem,0)-coalesce(ww.q_return,0)-coalesce(ww.q_return_rtl,0)-coalesce(ww.q_pri_supplier,0)-coalesce(ww.q_priperab,0)-coalesce(ww.q_pri_invent,0)-coalesce(ww.q_pri_prd,0)-coalesce(ww.q_pri_resort,0))/au.coefficient) q_debet_els,       
       ROUND(coalesce(SUM(ww.p_debet_ab),0)-coalesce(SUM(ww.p_problem),0)-coalesce(SUM(ww.p_return_pri),0)-coalesce(SUM(ww.p_return_rtl_pri),0)-coalesce(SUM(ww.p_pri_supplier),0)-coalesce(SUM(ww.p_priperab),0)-coalesce(SUM(ww.p_pri_invent),0)-coalesce(SUM(ww.p_pri_prd),0)-coalesce(SUM(ww.p_pri_resort),0),2) p_debet_els,
       coalesce(SUM(ww.q_pri_supplier/au.coefficient), 0) q_pri_supplier,
       ROUND(coalesce(SUM(ww.p_pri_supplier), 0), 2) p_pri_supplier, 
       coalesce(SUM(ww.q_priperab/au.coefficient), 0) q_priper, 
       ROUND(coalesce(SUM(ww.p_priperab), 0), 2) p_priper,
       coalesce(SUM(ww.q_pri_invent/au.coefficient), 0) q_pri_invent, 
       ROUND(coalesce(SUM(ww.p_pri_invent), 0), 2) p_pri_invent,       
       coalesce(SUM(ww.q_pri_invent_common/au.coefficient), 0) q_pri_invent_common, 
       ROUND(coalesce(SUM(ww.p_pri_invent_common), 0), 2) p_pri_invent_common,       
       coalesce(SUM(ww.q_pri_prd/au.coefficient), 0) q_pri_prd, 
       ROUND(coalesce(SUM(ww.p_pri_prd), 0), 2) p_pri_prd,
       coalesce(SUM(ww.q_pri_resort/au.coefficient), 0) q_pri_resort, 
       ROUND(coalesce(SUM(ww.p_pri_resort), 0), 2) p_pri_resort, 
       coalesce(SUM(ww.q_return/au.coefficient), 0) q_return, 
       ROUND(coalesce(SUM(ww.p_return_pri), 0), 2) p_return_pri,
       ROUND(coalesce(SUM(ww.p_return_pro), 0), 2) p_return_pro,             
       coalesce(SUM(ww.q_return_rtl/au.coefficient), 0) q_return_rtl, 
       ROUND(coalesce(SUM(ww.p_return_rtl_pri), 0), 2) p_return_rtl_pri,
       ROUND(coalesce(SUM(ww.p_return_rtl_pro), 0), 2) p_return_rtl_pro,             
       ROUND(SUM((coalesce(ww.q_problem, 0)+coalesce(rtl2.q_credit_ab, 0))/au.coefficient), 3) q_problem,
       ROUND(coalesce(SUM(ww.p_problem),0)+coalesce(SUM(rtl2.p_credit_ab_prihod),0),2) p_problem,        
       coalesce(SUM(ww.q_credit_ab/au.coefficient),0) q_credit_ab, 
       ROUND(coalesce(SUM(ww.p_credit_ab),0),2) p_credit_ab, 
       ROUND(coalesce(SUM(ww.p_creditab_prihod),0),2) p_creditab_prihod,
       coalesce(SUM((coalesce(ww.q_credit_ab_rtl,0)+coalesce(rtl2.q_credit_ab,0))/au.coefficient),0) q_credit_ab_rtl,
       ROUND(coalesce(SUM(ww.p_creditab_prihod_rtl),0)+coalesce(SUM(rtl2.p_credit_ab_prihod),0),2) p_credit_ab_rtl,       
       coalesce(SUM(ww.q_pered/au.coefficient), 0) q_pered, 
       ROUND(coalesce(SUM(ww.p_pered), 0), 2) p_pered,
       coalesce(SUM(ww.q_spis_invent/au.coefficient), 0) q_spis_invent, 
       ROUND(coalesce(SUM(ww.p_spis_invent), 0), 2) p_spis_invent,
       ROUND(coalesce(SUM(ww.p_spis_invent_sp), 0), 2) p_spis_invent_sp,         
       coalesce(SUM(ww.p_pri_invent_vat),0)-coalesce(SUM(ww.p_spis_invent_vat),0) p_invent_diff_vat,
       coalesce(SUM(ww.q_spis_prd/au.coefficient), 0) q_spis_prd,
       ROUND(coalesce(SUM(ww.p_spis_prd), 0), 2) p_spis_prd,
       ROUND(coalesce(SUM(ww.p_spis_prd_sp), 0), 2) p_spis_prd_sp,
       coalesce(SUM(ww.q_spis_resort/au.coefficient), 0) q_spis_resort,
       ROUND(coalesce(SUM(ww.p_spis_resort), 0), 2) p_spis_resort,         
       ROUND(coalesce(SUM(ww.p_spis_resort_sp), 0), 2) p_spis_resort_sp,         
       SUM((coalesce(ww.q_spis, 0)-coalesce(ww.q_spis_invent, 0)-coalesce(ww.q_spis_prd, 0)-coalesce(ww.q_spis_resort, 0))/au.coefficient) q_spis,      
       ROUND(SUM(coalesce(ww.p_spis, 0)-coalesce(ww.p_spis_invent, 0)-coalesce(ww.p_spis_prd, 0)-coalesce(ww.p_spis_resort, 0)), 2) p_spis,
       ROUND(SUM(coalesce(ww.p_spis_sp, 0)-coalesce(ww.p_spis_invent_sp, 0)-coalesce(ww.p_spis_prd_sp, 0)-coalesce(ww.p_spis_resort_sp, 0)), 2) p_spis_sp,
       coalesce(SUM(ww.q_retsup_ab/au.coefficient), 0) q_retsup, 
       ROUND(coalesce(SUM(ww.p_retsup_ab), 0), 2) p_retsup,       
       ROUND(coalesce(SUM(ww.p1_retsup_ab), 0), 2) p1_retsup_ab,       
       SUM((coalesce(ww.q_real, 0)-coalesce(ww.q_debet_bc, 0)+coalesce(ww.q_credit_bc, 0)+coalesce(ww.q_spisper_bc, 0)+coalesce(ww.q_retsup_bc, 0))/au.coefficient) q_end, 
       ROUND(SUM(coalesce(ww.p_real, 0)-coalesce(ww.p_debet_bc, 0)+coalesce(ww.p_creditbc_prihod, 0)+coalesce(ww.p_spisper_bc, 0)+coalesce(ww.p_retsup_bc, 0)), 2) p_end                                                                                                                            
  FROM 
    (SELECT 
       ss.code_shop,ss.number_shop,w.code_wares,w.name_wares,w.code_group 
     FROM 
       ods.v_wares_sat w,
       ods.v_subgroup_shop_sat ss 
     WHERE ss.code_shop IN (4)
       and current_date between w.open_date and w.close_date
       and current_date between ss.open_date and ss.close_date
     ) w
    inner join ods.v_addition_unit_sat au on au.code_wares = w.code_wares and current_date between au.open_date and au.close_date 
    inner join ods.v_unit_dimension_sat dau on dau.code_unit = au.code_unit and current_date between dau.open_date and dau.close_date 
    left join 
    (SELECT wi.code_shop,wi.code_wares, 
              SUM(wi.quantity_warehouse) q_real, 
              SUM(wi.quantity_warehouse*wi.price_coming_basis*(1+1*wi.vat)) p_real,
              coalesce(SUM(pr.q_debet_bc),0) q_debet_bc, 
              coalesce(SUM(pr.q_debet_ab),0) q_debet_ab,
              coalesce(SUM(pr.q_debet_bc*wi.price_coming_basis*(1+1*wi.vat)),0) p_debet_bc,
              coalesce(SUM(pr.q_debet_ab*wi.price_coming_basis*(1+1*wi.vat)),0) p_debet_ab, 
              coalesce(SUM(pr.q_return),0) q_return,
              coalesce(SUM(pr.q_return*wi.price_coming_basis*(1+1*wi.vat)),0) p_return_pri, 
              coalesce(SUM(pr.q_return*wi.price_by_invoice_basis*(1+1*wi.vat_invoice)),0) p_return_pro,
              coalesce(SUM(pr.q_return_rtl),0) q_return_rtl,
              coalesce(SUM(pr.q_return_rtl*wi.price_coming_basis*(1+1*wi.vat)),0) p_return_rtl_pri, 
              coalesce(SUM(pr.q_return_rtl*wi.price_by_invoice_basis*(1+1*wi.vat_invoice)),0) p_return_rtl_pro,
              coalesce(SUM(pr.q_pri_supplier),0) q_pri_supplier, 
              coalesce(SUM(pr.q_pri_supplier*wi.price_coming_basis*(1+1*wi.vat)),0) p_pri_supplier,
              coalesce(SUM(pr.q_priperab),0) q_priperab,
              coalesce(SUM(pr.q_priperab*wi.price_coming_basis*(1+1*wi.vat)),0) p_priperab,
              coalesce(SUM(pr.q_pri_invent),0) q_pri_invent, 
              coalesce(SUM(pr.q_pri_invent*wi.price_coming_basis*(1+1*wi.vat)),0) p_pri_invent, 
              coalesce(SUM(pr.q_pri_invent*wi.price_coming_basis*(1*wi.vat)),0) p_pri_invent_vat, 
              coalesce(SUM(pr.q_pri_invent_common),0) q_pri_invent_common, 
              coalesce(SUM(pr.q_pri_invent_common*wi.price_coming_basis*(1+1*wi.vat)),0) p_pri_invent_common, 
              coalesce(SUM(pr.q_pri_prd),0) q_pri_prd,
              coalesce(SUM(pr.q_pri_prd*wi.price_coming_basis*(1+1*wi.vat)),0) p_pri_prd, 
              coalesce(SUM(pr.q_pri_resort),0) q_pri_resort,
              coalesce(SUM(pr.q_pri_resort*wi.price_coming_basis*(1+1*wi.vat)),0) p_pri_resort,
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
              coalesce(SUM(spisper.p_spis_sp),0) p_spis_sp, 
              coalesce(SUM(spisper.q_r4),0) q_r4, 
              coalesce(SUM(spisper.q_r4*wi.price_coming_basis*(1+1*wi.vat)),0) p_r4,
              coalesce(SUM(spisper.q_spis_cli),0) q_spis_cli, 
              coalesce(SUM(spisper.q_spis_cli*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis_cli,
              coalesce(SUM(spisper.p_spis_cli_sp),0) p_spis_cli_sp, 
              coalesce(SUM(spisper.q_pered),0) q_pered, 
              coalesce(SUM(spisper.q_pered*wi.price_coming_basis*(1+1*wi.vat)),0) p_pered,
              coalesce(SUM(spisper.q_spis_invent),0) q_spis_invent,
              coalesce(SUM(spisper.q_spis_invent*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis_invent,
              coalesce(SUM(spisper.q_spis_invent*wi.price_coming_basis*(1*wi.vat)),0) p_spis_invent_vat,
              coalesce(SUM(spisper.sum_spis_invent),0) p_spis_invent_sp,
              coalesce(SUM(spisper.q_spis_prd),0) q_spis_prd,
              coalesce(SUM(spisper.q_spis_prd*wi.price_coming_basis*(1+1*wi.vat)),0) p_spis_prd,
              coalesce(SUM(spisper.sum_spis_prd),0) p_spis_prd_sp,
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
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,wi.quantity_in_basis, 0)) q_debet_bc,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,wi.quantity_in_basis)) q_debet_ab,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN ir.code_invoice IS NULL THEN DECODE(RTRIM(wi.type_invoice),'V',wi.quantity_in_basis,0) ELSE 0 END)) q_return,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN ir.code_invoice IS NOT NULL THEN wi.quantity_in_basis ELSE 0 END)) q_return_rtl,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(wi.type_source,'PO',wi.quantity_in_basis, 0))) q_pri_supplier,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(RTRIM(wi.type_invoice),'P',wi.quantity_in_basis, 0))) q_priperab,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(RTRIM(wi.problem),'Y',wi.quantity_in_basis, 0))) q_problem,
                     --SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (198,199,7367) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern) THEN wi.quantity_in_basis ELSE 0 END)) q_pri_invent,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts1.opt is not null THEN wi.quantity_in_basis ELSE 0 END)) q_pri_invent,
                     --SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (7367) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern) THEN wi.quantity_in_basis ELSE 0 END)) q_pri_invent_common,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts2.opt is not null THEN wi.quantity_in_basis ELSE 0 END)) q_pri_invent_common,
                     --SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (249,436,251,437) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern) THEN wi.quantity_in_basis ELSE 0 END)) q_pri_prd,
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts3.opt is not null THEN wi.quantity_in_basis ELSE 0 END)) q_pri_prd,
                     --SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (3523,3524) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (198,199,7367) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern)) THEN wi.quantity_in_basis ELSE 0 END)) q_pri_resort
                     SUM(DECODE(SIGN(wi.date_invoice::date-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts4.opt is not null THEN wi.quantity_in_basis ELSE 0 END)) q_pri_resort
            FROM ods.wares_invoice_link wi
              left join ods.invoice_rtl_order_link ir on wi.code_invoice = ir.code_invoice
              left join report_290_temp_table opts1 on opts1.opt IN (198,199,7367) AND opts1.code_shop=wi.code_shop AND opts1.code_pattern=wi.code_pattern
              left join report_290_temp_table opts2 on opts2.opt IN (7367) AND opts2.code_shop=wi.code_shop AND opts2.code_pattern=wi.code_pattern
              left join report_290_temp_table opts3 on opts3.opt IN (249,436,251,437) AND opts3.code_shop=wi.code_shop AND opts3.code_pattern=wi.code_pattern
              left join report_290_temp_table opts4 on opts4.opt IN (3523,3524) AND opts4.code_shop=wi.code_shop AND opts4.code_pattern=wi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table opts WHERE opts.opt IN (198,199,7367) AND opts.code_shop=wi.code_shop AND opts.code_pattern=wi.code_pattern)
            WHERE wi.date_invoice >= to_date('12.07.2023','dd.mm.yyyy') 
              AND wi.state_invoice = 'W'                  
              AND wi.code_shop IN (4)                  
              --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wi.code_wares and current_date between ws.open_date and ws.close_date)                
--              AND wi.code_wares IN ('60140')
            GROUP BY wi.code_wares, wi.code_invoice
            ) pr on pr.code_wares = wi.code_wares AND pr.code_invoice = wi.code_invoice
           left join 
           (SELECT wo.code_wares, wo.code_invoice, 
                     SUM(DECODE(SIGN(wo.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,wo.quantity_in_basis, 0)) q_credit_bc,
                     SUM(DECODE(SIGN(wo.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,wo.quantity_in_basis)) q_credit_ab,
                     SUM(DECODE(SIGN(wo.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,wo.quantity*(1+1*wo.vat)*wo.price)) p_credit_ab
            FROM ods.wares_out_invoice_link wo
            WHERE wo.date_from_warehouse >= to_date('12.07.2023','dd.mm.yyyy')
              AND wo.code_shop IN (4)
              --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wo.code_wares and current_date between ws.open_date and ws.close_date)                
--              AND wo.code_wares IN ('60140')
            GROUP BY wo.code_wares, wo.code_invoice
            ) ras on ras.code_wares = wi.code_wares AND ras.code_invoice = wi.code_invoice 
           left join 
           (SELECT wo.code_wares, wo.code_invoice, 
                     SUM(DECODE(SIGN(wo.date_retail_order-to_date('12.07.2023','dd.mm.yyyy')),1,wo.quantity_basis, 0)) q_credit_bc,
                     SUM(DECODE(SIGN(wo.date_retail_order-to_date('12.07.2023','dd.mm.yyyy')),1,0,wo.quantity_basis)) q_credit_ab--,
            FROM ods.rtl_retail_order_write_off_link wo
            WHERE wo.date_retail_order >= to_date('12.07.2023','dd.mm.yyyy')
              AND wo.code_shop IN (4)
              --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wo.code_wares and current_date between ws.open_date and ws.close_date)                
--              AND wo.code_wares IN ('60140')
            GROUP BY wo.code_wares, wo.code_invoice
            ) rtl on rtl.code_wares = wi.code_wares AND rtl.code_invoice = wi.code_invoice
           left join 
           (SELECT wwi.code_wares, wwi.code_invoice, 
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,wwi.quantity_in_basis, 0)) q_spisper_bc,                     
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity_in_basis ELSE 0 END,0)) q_spis_invent_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN opts5.opt is not null THEN wwi.quantity_in_basis ELSE 0 END,0)) q_spis_invent_bc,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (248,235,413) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity_in_basis ELSE 0 END,0)) q_spis_prd_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN opts6.opt is not null THEN wwi.quantity_in_basis ELSE 0 END,0)) q_spis_prd_bc,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (3525,6494,6493) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1  FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)) THEN wwi.quantity_in_basis ELSE 0 END,0)) q_spis_resort_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN opts7.opt is not null THEN wwi.quantity_in_basis ELSE 0 END,0)) q_spis_resort_bc,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END,0)) sum_spis_invent_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN opts8.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END,0)) sum_spis_invent_bc,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (248,235,413) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END,0)) sum_spis_prd_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN opts9.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END,0)) sum_spis_prd_bc,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (3525,6494,6493) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)) THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END,0)) sum_spis_resort_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,CASE WHEN opts10.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END,0)) sum_spis_resort_bc,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,wwi.quantity_in_basis)) q_spisper_ab,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.variety_write_off_invoice), 'T', 0, wwi.quantity_in_basis))) q_spis,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.variety_write_off_invoice), 'T', 0, wwi.quantity*(1+1*wwi.vat)*wwi.price))) p_spis_sp,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.type_write_off_invoice), 'R4', wwi.quantity_in_basis, 0))) q_r4,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(wwi.variety_write_off_invoice, 'SC', wwi.quantity_in_basis, 0))) q_spis_cli,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(wwi.variety_write_off_invoice, 'SC', wwi.quantity*(1+1*wwi.vat)*wwi.price, 0))) p_spis_cli_sp,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,DECODE(RTRIM(wwi.variety_write_off_invoice), 'T', wwi.quantity_in_basis, 0))) q_pered,                     
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_invent,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts11.opt is not null THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_invent,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (248,235,413) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_prd,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts12.opt is not null THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_prd,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (3525,6494,6493) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1  FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)) THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_resort,                     
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts13.opt is not null THEN wwi.quantity_in_basis ELSE 0 END)) q_spis_resort,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_invent,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts14.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_invent,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (248,235,413) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern) THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_prd,
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts15.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_prd,
                     --SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (3525,6494,6493) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern AND NOT EXISTS (SELECT 1 FROM report_290_temp_table2 opts WHERE opts.opt IN (200,6371,6370,7385) AND opts.code_shop=wwi.code_shop AND opts.code_pattern=wwi.code_pattern)) THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_resort
                     SUM(DECODE(SIGN(wwi.date_from_warehouse-to_date('12.07.2023','dd.mm.yyyy')),1,0,CASE WHEN opts16.opt is not null THEN wwi.quantity*(1+1*wwi.vat)*wwi.price ELSE 0 END)) sum_spis_resort
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
            WHERE wwi.date_from_warehouse >= to_date('12.07.2023','dd.mm.yyyy')
              AND wwi.code_shop IN (4)
              --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wwi.code_wares and current_date between ws.open_date and ws.close_date)                 
--              AND wwi.code_wares IN ('60140')
            GROUP BY wwi.code_wares, wwi.code_invoice
            ) spisper on spisper.code_wares = wi.code_wares AND spisper.code_invoice = wi.code_invoice
           left join 
           (SELECT wr.code_wares, wr.code_invoice code_invoice, 
                     SUM(DECODE(SIGN(wr.date_from_warehouse::date-to_date('12.07.2023','dd.mm.yyyy')), 1, wr.quantity_in_basis, 0)) q_retsup_bc,
                     SUM(DECODE(SIGN(wr.date_from_warehouse::date-to_date('12.07.2023','dd.mm.yyyy')), 1, 0, wr.quantity_in_basis)) q_retsup_ab,
                     SUM(DECODE(SIGN(wr.date_from_warehouse::date-to_date('12.07.2023','dd.mm.yyyy')), 1, 0, wr.quantity*(1+1*wr.vat)*wr.price)) p1_retsup_ab
            FROM ods.wares_in_return_invoice_link wr
            WHERE wr.date_from_warehouse >= to_date('12.07.2023','dd.mm.yyyy')
              AND wr.code_shop IN (4)
              --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wr.code_wares and current_date between ws.open_date and ws.close_date)                 
--              AND wr.code_wares IN ('60140')
            GROUP BY wr.code_wares, wr.code_invoice
            ) retsup on retsup.code_wares = wi.code_wares AND retsup.code_invoice = wi.code_invoice
        WHERE wi.state_invoice = 'W' 
          AND wi.code_shop IN (4)
          --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wi.code_wares and current_date between ws.open_date and ws.close_date)                
--          AND wi.code_wares IN ('60140')
        GROUP BY wi.code_shop,wi.code_wares
      ) ww on ww.code_shop = w.code_shop AND ww.code_wares = w.code_wares
    left join  
      (SELECT wo.code_shop,wo.code_wares, 
              SUM(wo.quantity_basis) q_credit_ab,
              SUM(wo.quantity_basis*(1+1*wo.vat)*wo.price_coming_forecast) p_credit_ab_prihod              
       FROM ods.rtl_retail_order_write_off_link wo
       WHERE wo.date_retail_order >= to_date('12.07.2023','dd.mm.yyyy')
         AND wo.date_retail_order <= to_date('12.07.2023','dd.mm.yyyy') 
         AND coalesce(wo.code_invoice, -1) = -1
         AND wo.code_shop IN (4)
         --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=wo.code_wares and current_date between ws.open_date and ws.close_date)                 
--         AND wo.code_wares IN ('60140')
       GROUP BY wo.code_shop,wo.code_wares
      ) rtl2 on rtl2.code_shop = w.code_shop AND rtl2.code_wares = w.code_wares 
WHERE 
  au.default_unit = 'Y' 
  --AND EXISTS (SELECT NULL FROM ods.v_wares_supplier_sat ws WHERE ws.code_supplier IN (5103452) AND ws.code_wares=w.code_wares and current_date between ws.open_date and ws.close_date)                 
--  AND w.code_wares IN ('60140')
GROUP BY w.number_shop
ORDER BY w.number_shop
"""

cur.execute(query)

print('Executed')

import pandas as pd

df1 = pd.DataFrame(cur.fetchall())

print(df1)

cur.close()
conn.close()