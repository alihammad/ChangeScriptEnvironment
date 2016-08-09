.RUN FILE = E:\SMB\report_scripts\template\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

/*  Bolton Adds 
/* Setting date range-- 15 days sliding window */
-- 26-04-2012 SP - modify sliding window
-- CMBS rate plan

CREATE VOLATILE TABLE dt_param
AS
( select
 (CURRENT_DATE - 15) AS start_dt,
CURRENT_DATE-2 AS end_dt
)
WITH  DATA
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
--select min(work_ord_completion_dt) from spcomm.f_bolton_add_churn

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- New actrivations for boltons ---
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE TABLE wo_ga
AS
( 
SELECT    
s.ext_id svc_no, 
svc.acct_id, 
svc.start_dt, 
ord.work_ord_no, 
ext_ord_ref_no,
ord.svc_inst_id, 
ord.work_ord_completion_dt, 
ord.work_ord_completion_tm, 
ord.work_ord_create_dt,
ord.work_ord_create_tm,
ord.work_ord_status, 
ord.work_ord_type, 
ord.work_ord_create_id,
sj.rep_id,
ord.SVC_ACTION_TYPE,
svc.svc_type,
s.ext_id_type,
rental_tariff_grp,
cmp.component_id, 
r.rate_plan_ds,
ord.dealer_cd,
CASE  WHEN cmp.bill_product_action = '1' THEN 'Provide'
           WHEN cmp.bill_product_action = '2' THEN 'Change'
           WHEN cmp.bill_product_action = '3' THEN 'Cancel'
           WHEN cmp.bill_product_action = '4' THEN 'Cease'
               ELSE 'NA'
	END cmp_bill_product_act_def,
cmp.active_dt cmp_active_dt, 
cmp.inactive_dt cmp_inactive_dt,
svc.svc_inst_status,
sales_chanl_id,
reps_id as wrkjn_rep_id,
retl_store
FROM      ipviews_srd.oo_svcins svc   ---select * from  ipviews_srd.oo_svcins where svc_inst_id = '100008459580'
	INNER JOIN  ipviews_srd.oo_wrkord ord
          ON  svc.svc_inst_id = ord.svc_inst_id
		 AND ord.work_ord_completion_dt BETWEEN svc.open_dt-2 AND svc.close_dt 
	INNER JOIN ipviews_srd.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
	INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
	INNER JOIN ipviews_srd.oo_svciex s -- select * from  ipviews_srd.oo_svciex where svc_inst_id = '100008459580'
	      ON  s.svc_inst_id = ord.svc_inst_id
          AND s.ext_id_type in ('20','601','15')     --601 DSLD; 15 - ULL
          AND s.start_dt <= ord.work_ord_completion_dt
          AND work_ord_completion_dt BETWEEN s.open_dt-2 AND s.close_dt
          AND work_ord_completion_dt BETWEEN s.start_dt AND COALESCE(s.end_dt, DATE '2899-12-31')
 	INNER JOIN ipcfg.cfg_rate_plan_ref r
          ON cmp.component_id =  r.rate_plan_cd
          and prod_grp_lvl_3 in ('VAS','Digital Business')
          and rate_plan_cd <> '700120'
 	INNER JOIN IPVIEWS_SRD.oo_svcijn sj
          ON  sj.svc_inst_id = ord.svc_inst_id 
	 AND work_ord_completion_dt BETWEEN sj.open_dt-2 AND sj.close_dt
	LEFT OUTER JOIN  ipviews_srd.OO_WRKOJN   ordjn  --- select * from ipviews_srd.OO_WRKOJN where work_ord_no = 'QRD0967003'
		ON ord.WORK_ORD_NO = ordjn.WORK_ORD_NO
		AND work_ord_completion_dt BETWEEN ordjn.open_dt-2 AND ordjn.close_dt
WHERE     ord.work_ord_status IN (4) -- completed
AND cmp.bill_product_action IN ('1','2') -- 1 provide, 2 - change
AND ord.work_ord_type IN ('1','2') -- 1 provide, 2 - change
AND svc_inst_status IN ('1','3') --  3 active, 1 new
AND svc_action_type NOT IN ('40','1') -- 40 migraiton, 1 - SI transfer
AND cmp_active_dt IS NOT NULL
AND work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)
QUALIFY RANK() OVER (
PARTITION BY ord.work_ord_no
                            ORDER BY  work_ord_completion_dt DESC, work_ord_completion_tm DESC,  svc.close_dt desc, s.open_dt DESC, s.start_dt desc, sj.open_dt asc,ordjn.open_dt asc) = 1 
) WITH DATA
PRIMARY INDEX ( svc_no ,ACCT_ID ,WORK_ORD_COMPLETION_DT , COMPONENT_ID ,cmp_active_dt )
ON COMMIT PRESERVE ROWS 
;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

/*select work_ord_no, component_id, count(*)  from wo_ga group by 1,2 having count(*) > 1

select * from wo_ga where work_ord_no = 'UGG9793001'
select * from 
ipviews_srd.oo_btxnpa pkg
	INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
          left outer join ipviews_srd.AR_COMPONENT_DEFINITION_VALUES a
          on cmp.component_id = a.component_id
where work_ord_no = 'UGG9793001'*/
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- pick up VIP code for new adds
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE VOLATILE TABLE ga_vip
AS
( 
SELECT wo_ga.*,
bill_lname,
CAST(vip_code AS VARCHAR(25)) AS vip_code,
 OWNING_COST_CTR,
 OWNING_COST_CTR_DV,
bill_zip
FROM
wo_ga
LEFT OUTER JOIN
ipviews_srd.ar_a200 a200
ON wo_ga.acct_id = a200.external_id 
AND a200.external_id_type='1'
AND a200.close_dt = '2899-12-31'
LEFT OUTER JOIN  ipviews_srd.ar_a100 a100
 ON  a200.account_no=a100.account_no
 AND wo_ga.WORK_ORD_COMPLETION_DT BETWEEN a100.open_dt -15 AND a100.close_dt
QUALIFY  RANK() OVER (
       PARTITION BY  wo_ga.svc_no, wo_ga.acct_id, wo_ga.component_id,work_ord_no
       ORDER BY   a100.close_dt DESC) = 1
       ) WITH DATA
       PRIMARY INDEX ( svc_no ,WORK_ORD_COMPLETION_DT )
             ON COMMIT PRESERVE ROWS
;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CFU derivation
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE MULTISET TABLE vt_ga as (
select 
svc_no,
acct_id as acct_no,
bill_lname as customer_name,
component_id as bolton_rate_plan,
rate_plan_ds as bolton_rate_plan_ds,
ext_id_type,
svc_type,
sc.cfu,
vip_code,
owning_cost_ctr,
owning_cost_ctr_dv,
rt.decoded_value as rental_tariff_group,
work_ord_create_id,
rep_id,
dealer_cd,
bill_zip as post_cd,
work_ord_no,
ext_ord_ref_no,
svc_inst_id,
cmp_active_dt as bolton_active_dt,
cmp_inactive_dt as bolton_inactive_dt,
work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_tm,
work_ord_type,
cmp_bill_product_act_def as bill_product_action,
mc.decoded_value as svc_action_type,
sales_chanl_id,
wrkjn_rep_id,
retl_store
from
ga_vip
LEFT OUTER JOIN 
ipviews_md.md_sales_class_ref sc
ON ga_vip.vip_code = sc.sales_class_ref_cd
AND sc.close_dt = '2899-12-31'
left outer join ipviews_srd.oo_reftmc mc
on ga_vip.svc_action_type = mc.mstr_cd
and mc.close_dt = DATE '2899-12-31'  
and mc.mstr_cd_key_type = 'svc_action_ty'
left outer join ipviews_srd.oo_reftmc rt
on ga_vip.rental_tariff_grp = rt.mstr_cd
and rt.mstr_cd_key_type = 'RENTAL_TARIFF_GRP'
and rt.close_dt = DATE '2899-12-31'  
) WITH DATA PRIMARY INDEX (svc_no, work_ord_completion_dt)
ON COMMIT PRESERVE ROWS;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- basic rate plan derivation for new adds
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- fetch subscr no

create volatile table ga_subscr as (
select 
vt_ga.svc_no, work_ord_no, s200.subscr_no, work_ord_completion_dt
from vt_ga
INNER JOIN ipviews_srd.ar_s200 s200
ON vt_ga.svc_no = s200.external_id 
AND vt_ga.ext_id_type = s200.external_id_type
AND work_ord_completion_dt BETWEEN cast(s200.active_date as date) -2 AND COALESCE(s200.inactive_date, DATE '2899-12-31')
AND s200.record_type <> '40'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  s200.close_dt desc, s200.active_date desc  ) = 1 
) with data primary index (svc_no, subscr_no)
on commit preserve rows;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
-- fetch basic plans


CREATE VOLATILE TABLE ga_basic AS (
SELECT 
ga_subscr.svc_no, work_ord_no, ga_subscr.subscr_no,   p200.component_id AS basic_rate_plan_cd, ar.component_id_dv, ar.short_display,work_ord_completion_dt
FROM ga_subscr
LEFT OUTER JOIN ipviews_srd.ar_p200 p200 
ON p200.subscr_no = ga_subscr.subscr_no
AND work_ord_completion_dt BETWEEN p200.product_start AND COALESCE(p200.product_stop, DATE '2899-12-31')
AND p200.close_dt = '2899-12-31'
INNER JOIN ipviews_srd.AR_PACKAGE_DEFINITION_REF pac
ON p200.package_id = pac.package_id
AND pac.package_group IN ('5','6','3','4','22')
AND pac.close_dt = '2899-12-31'
INNER JOIN
ipviews_srd.AR_COMPONENT_DEFINITION_VALUES ar
ON p200.component_id =ar.component_id
AND ar.close_dt = '2899-12-31'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  p200.product_start DESC, COALESCE(p200.product_stop, DATE '2899-12-31') DESC ) = 1    
) WITH DATA PRIMARY INDEX (svc_no, subscr_no, basic_rate_plan_cd)
ON COMMIT PRESERVE ROWS ;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- consolidating  adds

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


create volatile multiset table vt_ga_fin as (
select 
vt_ga.svc_no,
vt_ga.acct_no,
customer_name,
bolton_rate_plan as bolton_rate_plan_cd,
bolton_rate_plan_ds,
cast(trim(basic_rate_plan_cd) as varchar(15)) as basic_rate_plan_cd,
component_id_dv as basic_plan_ds,
ext_id_type,
svc_type,
cfu,
vip_code,
owning_cost_ctr,
owning_cost_ctr_dv,
rental_tariff_group,
work_ord_create_id,
rep_id,
dealer_cd,
post_cd,
vt_ga.work_ord_no,
vt_ga.ext_ord_ref_no,
svc_inst_id,
subscr_no,
bolton_active_dt,
bolton_inactive_dt,
vt_ga.work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_tm,
work_ord_type,
bill_product_action,
svc_action_type,
vt_ga.sales_chanl_id,
vt_ga.wrkjn_rep_id,
vt_ga.retl_store
from
vt_ga
left outer join 
ga_basic
on vt_ga.svc_no = ga_basic.svc_no
and vt_ga.work_ord_no = ga_basic.work_ord_no
) with data primary index (svc_no)
on commit preserve rows;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*bolton churns */

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- pick up churn
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE TABLE wo_churn AS (
SELECT    
s.ext_id svc_no, 
svc.acct_id, 
svc.start_dt, 
ord.work_ord_no, 
ext_ord_ref_no,
ord.svc_inst_id, 
ord.work_ord_completion_dt, 
ord.work_ord_completion_tm, 
ord.work_ord_create_dt,
ord.work_ord_create_tm,
ord.work_ord_status, 
ord.work_ord_type, 
ord.work_ord_create_id,
sj.rep_id,
ord.SVC_ACTION_TYPE,
svc.svc_type,
s.ext_id_type,
rental_tariff_grp,
cmp.component_id, 
r.rate_plan_ds,
ord.dealer_cd,
CASE  WHEN cmp.bill_product_action = '1' THEN 'Provide'
           WHEN cmp.bill_product_action = '2' THEN 'Change'
           WHEN cmp.bill_product_action = '3' THEN 'Cancel'
           WHEN cmp.bill_product_action = '4' THEN 'Cease'
               ELSE 'NA'
	END cmp_bill_product_act_def,
cmp.active_dt cmp_active_dt, 
cmp.inactive_dt cmp_inactive_dt,
svc.svc_inst_status,
decoded_value AS disconnect_reason, 
ord.disconnect_cd,
sales_chanl_id,
reps_id as wrkjn_rep_id,
retl_store
FROM      ipviews_srd.oo_svcins svc
    INNER JOIN ipviews_srd.oo_wrkord ord
		ON  svc.svc_inst_id = ord.svc_inst_id
         and   ord.work_ord_close_dt BETWEEN svc.open_dt-2 AND svc.close_dt
    INNER JOIN ipviews_srd.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
     INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
     INNER JOIN ipviews_srd.oo_svciex s   -- select * from ipviews_srd.oo_svciex where svc_inst_id = '100002407653'
          ON  s.svc_inst_id = ord.svc_inst_id
          AND s.ext_id_type in ('20','601','15')     --601 DSLD; 15 - ULL
          AND s.start_dt <= ord.work_ord_completion_dt
          AND work_ord_completion_dt BETWEEN s.open_dt-2 AND s.close_dt
          AND work_ord_completion_dt BETWEEN s.start_dt AND COALESCE(s.end_dt, DATE '2899-12-31')
    iNNER JOIN ipcfg.cfg_rate_plan_ref r
          ON cmp.component_id =  r.rate_plan_cd
          and prod_grp_lvl_3 in ('VAS','Digital Business')
          and rate_plan_cd <> '700120'
     INNER JOIN IPVIEWS_SRD.oo_svcijn sj
          ON  sj.svc_inst_id = ord.svc_inst_id 
     AND work_ord_completion_dt BETWEEN sj.open_dt-1 AND sj.close_dt
           LEFT OUTER JOIN 
 ipviews_srd.OO_REFTmc ref
 ON ord.disconnect_cd = ref.mstr_cd
AND ref.close_dt = '2899-12-31'
AND mstr_cd_key_type = 'disconnect_cd '
	LEFT OUTER JOIN  ipviews_srd.OO_WRKOJN   ordjn  --- select * from ipviews_srd.OO_WRKOJN where work_ord_no = 'QRD0967003'
		ON ord.WORK_ORD_NO = ordjn.WORK_ORD_NO
		AND work_ord_completion_dt BETWEEN ordjn.open_dt-2 AND ordjn.close_dt
WHERE     ord.work_ord_status IN (4) -- completed
AND cmp.bill_product_action IN ('3','4') -- 3 cancel, 4 - cease
AND svc_action_type NOT IN ('40','1') -- 40 migraiton, 1 - SI transfer
AND cmp_inactive_dt IS NOT NULL
--AND svc.svc_inst_status  IN ('1','3')
AND work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)
QUALIFY RANK() OVER (
PARTITION BY ord.work_ord_no, component_id
                            ORDER BY  work_ord_completion_dt DESC, work_ord_completion_tm DESC,  svc.close_dt desc, s.close_dt DESC,COALESCE(s.end_dt, DATE '2899-12-31') desc , s.start_dt desc, sj.close_dt asc, ordjn.close_dt desc, cmp_inactive_dt asc ) = 1 
) WITH DATA PRIMARY INDEX (svc_no, acct_id, component_id,cmp_active_dt,work_ord_completion_dt)
ON COMMIT PRESERVE ROWS ;

--select work_ord_no, component_id, cmp_active_dt, count(*)  from wo_churn  group by 1,2,3 having count(*) > 1

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--vip code

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE TABLE churn_vip
AS
( 
SELECT wo_churn.*,
bill_lname,
CAST(vip_code AS VARCHAR(25)) AS vip_code,
 OWNING_COST_CTR,
 OWNING_COST_CTR_DV,
bill_zip
FROM
wo_churn
LEFT OUTER JOIN
ipviews_srd.ar_a200 a200
ON wo_churn.acct_id = a200.external_id 
AND a200.external_id_type='1'
AND a200.close_dt = '2899-12-31'
LEFT OUTER JOIN  ipviews_srd.ar_a100 a100
 ON  a200.account_no=a100.account_no
 AND wo_churn.WORK_ORD_COMPLETION_DT BETWEEN a100.open_dt -15 AND a100.close_dt
QUALIFY  RANK() OVER (
       PARTITION BY  wo_churn.svc_no, wo_churn.acct_id, wo_churn.component_id,work_ord_no
       ORDER BY   a100.close_dt DESC) = 1
       ) WITH DATA
       PRIMARY INDEX ( svc_no ,WORK_ORD_COMPLETION_DT )
             ON COMMIT PRESERVE ROWS
;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- consolidate churn

CREATE VOLATILE MULTISET TABLE vt_churn as (
select 
svc_no,
acct_id as acct_no,
bill_lname as customer_name,
component_id as bolton_rate_plan,
rate_plan_ds as bolton_rate_plan_ds,
ext_id_type,
svc_type,
sc.cfu,
vip_code,
owning_cost_ctr,
owning_cost_ctr_dv,
rt.decoded_value as rental_tariff_group,
work_ord_create_id,
rep_id,
dealer_cd,
bill_zip as post_cd,
work_ord_no,
ext_ord_ref_no,
svc_inst_id,
cmp_active_dt as bolton_active_dt,
cmp_inactive_dt as bolton_inactive_dt,
work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_tm,
work_ord_type,
cmp_bill_product_act_def as bill_product_action,
mc.decoded_value as svc_action_type,
sales_chanl_id,
wrkjn_rep_id,
retl_store,
disconnect_reason,
disconnect_cd
from
churn_vip
LEFT OUTER JOIN 
ipviews_md.md_sales_class_ref sc
ON churn_vip.vip_code = sc.sales_class_ref_cd
AND sc.close_dt = '2899-12-31'
left outer join ipviews_srd.oo_reftmc mc
on churn_vip.svc_action_type = mc.mstr_cd
and mc.close_dt = DATE '2899-12-31'  
and mc.mstr_cd_key_type = 'svc_action_ty'
left outer join ipviews_srd.oo_reftmc rt
on churn_vip.rental_tariff_grp = rt.mstr_cd
and rt.mstr_cd_key_type = 'RENTAL_TARIFF_GRP'
and rt.close_dt = DATE '2899-12-31'  
) WITH DATA PRIMARY INDEX (svc_no, work_ord_completion_dt)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- basic rate plan derivation for churns
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- fetch subscr no


create volatile table churn_subscr as (
select 
vt_churn.svc_no, work_ord_no, s200.subscr_no, work_ord_completion_dt
from vt_churn
INNER JOIN ipviews_srd.ar_s200 s200
ON vt_churn.svc_no = s200.external_id 
AND vt_churn.ext_id_type = s200.external_id_type
AND work_ord_completion_dt BETWEEN s200.open_dt -3  AND  s200.close_dt
AND work_ord_completion_dt BETWEEN s200.active_date  AND  coalesce(s200.inactive_date, date '2899-12-31') +3
AND s200.record_type <> '40'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  s200.close_dt desc, s200.inactive_date desc  ) = 1 
) with data primary index (svc_no, subscr_no)
on commit preserve rows;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- fetch basic plans


CREATE VOLATILE TABLE churn_basic AS (
SELECT 
churn_subscr.svc_no, work_ord_no, churn_subscr.subscr_no,   p200.component_id AS basic_rate_plan_cd, ar.component_id_dv, ar.short_display,work_ord_completion_dt
FROM churn_subscr
LEFT OUTER JOIN ipviews_srd.ar_p200 p200 
ON p200.subscr_no = churn_subscr.subscr_no
AND work_ord_completion_dt BETWEEN p200.product_start AND COALESCE(p200.product_stop, DATE '2899-12-31')
INNER JOIN ipviews_srd.AR_PACKAGE_DEFINITION_REF pac
ON p200.package_id = pac.package_id
AND pac.package_group IN ('5','6','3','4','22')
AND pac.close_dt = '2899-12-31'
INNER JOIN
ipviews_srd.AR_COMPONENT_DEFINITION_VALUES ar
ON p200.component_id =ar.component_id
AND ar.close_dt = '2899-12-31'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  p200.product_start DESC, COALESCE(p200.product_stop, DATE '2899-12-31') DESC, p200.close_dt desc ) = 1    
) WITH DATA PRIMARY INDEX (svc_no, subscr_no, basic_rate_plan_cd)
ON COMMIT PRESERVE ROWS ;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---consolidate churns 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


create volatile multiset table vt_churn_fin as (
select 
'Churns' as status_cd,
vt_churn.svc_no,
vt_churn.acct_no,
customer_name,
bolton_rate_plan as bolton_rate_plan_cd,
bolton_rate_plan_ds,
cast(trim(basic_rate_plan_cd) as varchar(15)) as basic_rate_plan_cd,
component_id_dv as basic_plan_ds,
ext_id_type,
svc_type,
cfu,
vip_code,
owning_cost_ctr,
owning_cost_ctr_dv,
rental_tariff_group,
work_ord_create_id,
rep_id,
dealer_cd,
post_cd,
vt_churn.work_ord_no,
ext_ord_ref_no,
svc_inst_id,
subscr_no,
bolton_active_dt,
bolton_inactive_dt,
vt_churn.work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_tm,
work_ord_type,
bill_product_action,
svc_action_type,
sales_chanl_id,
wrkjn_rep_id,
retl_store,
disconnect_reason,
disconnect_cd
from
vt_churn
left outer join 
churn_basic
on vt_churn.svc_no = churn_basic.svc_no
and vt_churn.work_ord_no = churn_basic.work_ord_no
) with data primary index (svc_no)
on commit preserve rows;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
--- find original dealer code and rep id


CREATE VOLATILE TABLE orig_wo
AS
( 
SELECT    
ord.work_ord_no,
ord.svc_inst_id, 
ord.work_ord_completion_dt, 
ord.work_ord_completion_tm, 
sj.rep_id,
cmp.component_id, 
ord.dealer_cd,
CASE  WHEN cmp.bill_product_action = '1' THEN 'Provide'
           WHEN cmp.bill_product_action = '2' THEN 'Change'
           WHEN cmp.bill_product_action = '3' THEN 'Cancel'
           WHEN cmp.bill_product_action = '4' THEN 'Cease'
               ELSE 'NA'
	END cmp_bill_product_act_def,
cmp.active_dt cmp_active_dt, 
cmp.inactive_dt cmp_inactive_dt,
reps_id as wrkjn_rep_id
FROM    
vt_churn_fin vt
	inner join
   		 ipviews_srd.oo_svcins svc   ---select * from  ipviews_srd.oo_svcins where svc_inst_id = '100008626232'
		on vt.svc_inst_id = svc.svc_inst_id
	INNER JOIN  ipviews_srd.oo_wrkord ord
          ON  svc.svc_inst_id = ord.svc_inst_id
		 --AND ord.work_ord_completion_dt <= vt.work_ord_completion_dt
	INNER JOIN ipviews_srd.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
	INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
left outer  JOIN IPVIEWS_SRD.oo_svcijn sj
          ON  sj.svc_inst_id = ord.svc_inst_id 
	 AND ord.work_ord_completion_dt BETWEEN sj.open_dt-1 AND sj.close_dt
	LEFT OUTER JOIN  ipviews_srd.OO_WRKOJN   ordjn  --- select * from ipviews_srd.OO_WRKOJN where work_ord_no = 'QRD0967003'
		ON ord.WORK_ORD_NO = ordjn.WORK_ORD_NO
		AND ord.work_ord_completion_dt BETWEEN ordjn.open_dt-2 AND ordjn.close_dt
WHERE     ord.work_ord_status IN (4) -- completed
AND cmp.bill_product_action IN ('1','2') -- 1 provide, 2 - change
AND ord.work_ord_type IN ('1','2') -- 1 provide, 2 - change
AND ord.svc_action_type NOT IN ('40','1') -- 40 migraiton, 1 - SI transfer
and cmp.inactive_dt is null
AND cmp_active_dt = vt.bolton_active_dt
and cmp.component_id = vt.bolton_rate_plan_cd
QUALIFY RANK() OVER (
PARTITION BY component_id,vt.svc_inst_id
                            ORDER BY  ord.work_ord_completion_dt DESC, ord.work_ord_completion_tm DESC,  ordjn.open_dt asc,ordjn.open_dt asc) = 1 
) WITH DATA
PRIMARY INDEX ( svc_inst_id , COMPONENT_ID ,cmp_active_dt )
ON COMMIT PRESERVE ROWS ;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

create volatile table vt_churn_fin_2 as (
select 
f.*,
o.dealer_cd as orginal_dlr_cd,
o.wrkjn_rep_id as orginal_rep_id
 from vt_churn_fin f
left outer join 
orig_wo o
on f.svc_inst_id = o.svc_inst_id
and f.bolton_rate_plan_cd = o.component_id
and f.bolton_active_dt = o.cmp_active_dt
) with data primary index (svc_no)
on commit preserve rows;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

delete from spcomm.f_bolton_add_churn 
where work_ord_completion_dt between (SEL start_dt from dt_param) and (sel end_dt from dt_param)
and src_tbl = 'Opom Extract';

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn 
select a.*, null as no_of_svc_x_acct, current_date as load_date, 'Opom Extract' as src_tbl  from vt_churn_fin_2 a ;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn
select 'Gross Adds', a.*, null as disconnect_reason, null as disconnect_cd, null as orginal_dlr_cd, null as orginal_rep_id, null as no_of_svc_x_acct, current_date as load_date, 'Opom Extract' as src_tbl from vt_ga_fin a ;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE



----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- METV ---
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


create volatile multiset table metv as (
SELECT
case when action_type = 'DISCONNECTION' then 'Churns'
			when action_type = 'Activation' then 'Gross Adds'
			when action_type = 'Order' then 'Orders Submitted'
			when action_type = 'Cancellation' then 'Orders Cancelled'
			end as event
,service_id
,account_number
,cast(null as varchar(50)) as customer_name
,case when service_type = 'ONC' then CMBS_rate_plan_id else opom_component_id end  as bolton_rate_plan_cd
,product_code as bolton_rate_plan_ds
,cast(CASE WHEN POSITION('_' IN sys_rate_plan_cd) > 0    
                        THEN SUBSTRING(sys_rate_plan_cd FROM POSITION('_' IN sys_rate_plan_cd)+1 FOR POSITION('_' IN SUBSTRING(sys_rate_plan_cd
                           FROM POSITION('_' IN sys_rate_plan_cd)+1))-1)
                    ELSE sys_rate_plan_cd
                  END as varchar(32))  as basic_rate_plan_cd
,coalesce(plan_name, sys_rate_plan_nm) as basic_plan_ds
,null as ext_id_type
,null as svc_type
,cfu
,null as vip_code
,null as owning_cost_ctr
,null as owning_cost_ctr_dv
,null as rental_tariff_group
,null as work_ord_create_id
,rep_id
,dealer_id
,addr_post_cd as post_cd
,work_ord_no
,sos_order_no
,null as svc_inst_id
,null as subscr_no
,transaction_date as bolton_active_dt
,null as bolton_inactive_dt
,transaction_date as work_ord_completion_dt
,null as work_ord_completion_tm
,null as work_ord_create_dt
,null as work_ord_create_tm
,null as work_ord_type
,null as bill_product_action
,null as svc_action_type
,opom_sales_chanl_id as sales_chanl_id
,null as wrjn_rep_id
,null as retl_store
,null as disconnect_reason
,null as disconnect_cd
from IPSHARE.OTT_AT_TRANSACTION   --- select * from ipshare.ott_at_transaction where service_id    ='sagrawal' = 'buzzabuzza'
where transaction_date between (select start_dt from dt_param) and (select end_dt from dt_param)
and action_type in (
'ACTIVATION',
'CANCELLATION',
'ORDER',
'DISCONNECTION')
QUALIFY RANK() OVER (
PARTITION BY event, service_id, bolton_rate_plan_cd, transaction_date
                            ORDER BY dealer_id desc, rep_id desc ) = 1  
) with data primary index (service_id)
on commit preserve rows;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- fetch customer_name

update a
from metv a,
(select acct_no, acct_name from ipviews.acct 
where close_dt = '2899-12-31'
QUALIFY RANK() OVER (
PARTITION BY acct_name
                            ORDER BY acct_no desc ) = 1  
) p
set customer_name = acct_name
where account_number = p.acct_no
;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- delete statement

delete from spcomm.f_bolton_add_churn 
where work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param)
and src_tbl = 'OTT Trans';

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--- insert all except order

insert into
spcomm.f_bolton_add_churn
select 
metv.* ,
x.dealer_id as x_dlr_cd,
x.rep_id as x_rep_id,
null as no_of_svc_x_acct,
current_date as load_date,
'OTT Trans' as src_tbl
from 
metv
left outer join (
select 
transaction_date,
service_id,
dealer_id,
rep_id
from 
IPSHARE.OTT_AT_TRANSACTION 
where action_type  = 'ORDER'
QUALIFY RANK() OVER (
PARTITION BY service_id, transaction_date
                            ORDER BY dealer_id desc, rep_id desc) = 1  
) x
on metv.service_id = x.service_id
and metv.bolton_active_dt >= x.transaction_date
where metv.event in (
'Gross Adds',
'Orders Cancelled',
'Churns')
QUALIFY RANK() OVER (
PARTITION BY metv.service_id, metv.event, metv.bolton_active_dt
                            ORDER BY  x.transaction_date asc) = 1  
;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE                            

-- insert orders


insert into
spcomm.f_bolton_add_churn
select 
metv.* ,
null as x_dlr_cd,
null as x_rep_id,
null as no_of_svc_x_acct,
current_date as load_date,
'OTT Trans' as src_tbl
from 
metv
where event = 'Orders Submitted';


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE




----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- FBB bolton speed pack  ---
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

delete from spcomm.f_bolton_add_churn
where src_tbl = 'adac_fbb'
and work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param);


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn
select
case when svc_stat_cd = 'A' then 'Gross Adds'
			when svc_stat_cd ='C' then 'Churns'
			else svc_stat_cd end as status_cd
,svc_no
,acct_no
,bill_to_acct_nm
,ref.rate_plan_cd as bolton_rate_plan
,ref.rate_plan_ds as bolton_rate_plan_ds
,basic_accs_rate_plan_cd
,component_id_dv as basic_rate_plan_ds
,null as ext_id
,null as svc_type
,cfu 
,null as vip_code
,null as owning_cost_ctr
,null as owning_cost_ctr_dv
, null as rental_tariff_group
, null as work_ord_create_id
,rep_cd as rep_id
,dlr_cd
,cust_post_cd
,ref_no as work_ord_no
,null as ext_ord_ref_no
,null as svc_inst_id
,null as subscr_no
,day_dt as bolton_active_dt
,null as bolton_inactive_dt
,day_dt as work_ord_completion_dt
,null as work_ord_completion_tm
,null as work_ord_create_dt
,null as work_ord_create_tm
,null as work_ord_type
,null as bill_product_action
,null as svc_action_type
,null as sales_chanl_id
,null as wrkjn_rep_id
,null as retl_store
,rsn_cd as disconnect_reason
,null as disconnect_cd
,null as original_dlr_cd
,null as original_rep_id
,null as no_of_svc_x_acct
,current_date as load_date
,'adac_fbb' as src_tbl
from spcomm.all_daily_add_churn ac
left outer join spcomm.cfg_sov_ref ref
	on ac.contrct_nm = ref.rate_plan_ds
	and ref.end_dt = '2899-12-31'
left outer join ipviews_srd.AR_COMPONENT_DEFINITION_VALUES com
	on ac.basic_accs_rate_plan_cd = com.component_id
	and com.close_dt = '2899-12-31'
where src_tbl in ('fixed_fbb_adds_stg','fixed_fbb_canx_stg')
and day_dt between (select start_dt from dt_param) and (select end_dt from dt_param)
and ref.rate_plan_cd is not null;


--- mobile data boltons  ---


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

delete from spcomm.f_bolton_add_churn
where src_tbl = 'adac_mobdata'
and work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn
select 
case when service_event_grp = 'Gross Adds' then 'Gross Adds'
		  when service_event_grp = 'Churns' then 'Churns'
		  else null end as status_cd
,svc_no
,acct_no
,bill_to_acct_nm
,bolton_rate_plan_cd
,r.rate_plan_ds as bolton_rate_plan_ds
,CASE 
                    WHEN POSITION('_' IN basic_accs_rate_plan_cd) > 0    
                    THEN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1 FOR POSITION('_' IN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1))-1)
                    ELSE basic_accs_rate_plan_cd
                  END   as basic_rate_plan_cd
,b.rate_plan_ds as basic_plan_ds
,null as ext_id_type
,null as svc_type
,cfu
,null as vip_code
,null as owning_cost_ctr
,null as owning_cost_ctr_dv
,null as rental_tariff_group
, null as work_ord_create_id
,rep_cd
,dlr_cd
,cust_post_cd
,null as work_ord_no
,null as ext_ord_ref_no
,null as svc_inst_id
,null as subscr_no
,case when service_event_grp = 'Gross Adds' then day_dt else null end as bolton_active_dt
,case when service_event_grp = 'Churns' then day_dt else null end  as bolton_inactive_dt
,day_dt as work_ord_completion_dt
, null as work_ord_completion_tm
, null as work_ord_create_dt
, null as work_ord_create_tm
,null as work_ord_type
, null as bill_product_action
,null as svc_action_type
, null as sales_chanl_id
, null as wrkjn_rep_id
, null as retl_store
,null as disconnect_reason
,null as disconnect_cd
,null as original_drl_cd
,null as original_rep_id
,no_of_svc_x_acct
,current_date as load_date
,'adac_mobdata' src_tbl
from spcomm.all_daily_add_churn ac
inner join ipcfg.cfg_rate_plan_ref r
on ac.bolton_rate_plan_cd = r.rate_plan_cd
inner join bpviews_app.L_SERVICE_EVENT se
on ac.svc_stat_cd = service_event_cd
left outer join ipcfg.cfg_rate_plan_ref b 
on basic_rate_plan_cd = b.rate_plan_cd
where ac.src_tbl = 'mob_mobile_data'
and ac.day_dt between (select start_dt from dt_param) and (select end_dt from dt_param)
and se.service_event_grp in ('Gross Adds','Churns')
and r.prod_grp_lvl_1 = 'Mobile Internet Data Bolt On'
and ac.cfu in ('SMB','Consumer')
and status_cd is not null;



--- blackberry boltons  ---


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

delete from spcomm.f_bolton_add_churn
where src_tbl = 'adac_mob_bb'
and work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param);

--.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn
select 
case when service_event_grp = 'Gross Adds' then 'Gross Adds'
		  when service_event_grp = 'Churns' then 'Churns'
		  else null end as status_cd
,svc_no
,acct_no
,bill_to_acct_nm
,bolton_rate_plan_cd
,r.rate_plan_ds as bolton_rate_plan_ds
,CASE 
                    WHEN POSITION('_' IN basic_accs_rate_plan_cd) > 0    
                    THEN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1 FOR POSITION('_' IN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1))-1)
                    ELSE basic_accs_rate_plan_cd
                  END   as basic_rate_plan_cd
,b.rate_plan_ds as basic_plan_ds
,null as ext_id_type
,null as svc_type
,cfu
,null as vip_code
,null as owning_cost_ctr
,null as owning_cost_ctr_dv
,null as rental_tariff_group
, null as work_ord_create_id
,rep_cd
,dlr_cd
,cust_post_cd
,null as work_ord_no
,null as ext_ord_ref_no
,null as svc_inst_id
,null as subscr_no
,day_dt as bolton_active_dt
,day_dt as bolton_inactive_dt
,day_dt as work_ord_completion_dt
, null as work_ord_completion_tm
, null as work_ord_create_dt
, null as work_ord_create_tm
,null as work_ord_type
, null as bill_product_action
,null as svc_action_type
, null as sales_chanl_id
, null as wrkjn_rep_id
, null as retl_store
,null as disconnect_reason
,null as disconnect_cd
,null as original_drl_cd
,null as original_rep_id
,no_of_svc_x_acct
,current_date as load_date
,'adac_mob_bb' src_tbl
from spcomm.all_daily_add_churn ac
inner join ipcfg.cfg_rate_plan_ref r
on ac.bolton_rate_plan_cd = r.rate_plan_cd
inner join bpviews_app.L_SERVICE_EVENT se
on ac.svc_stat_cd = service_event_cd
left outer join ipcfg.cfg_rate_plan_ref b
on basic_rate_plan_cd = b.rate_plan_cd
where src_tbl = 'mob_blackberry'
and day_dt between (select start_dt from dt_param) and (select end_dt from dt_param)
and service_event_grp in ('Gross Adds','Churns')
and r.commissionable_in = 'Y'
and r.bb_cnt_in = '1'
and r.bolton_in ='1'
and r.prod_grp_lvl_1 = 'Blackberry'
and ac.cfu in ('SMB','Consumer')
;

-- account level boltons -- 


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

delete from spcomm.f_bolton_add_churn
where src_tbl = 'oo_awkord'
and work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param);


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


create volatile table tbc_wo as (
select 
wkr.work_ord_no,
wkr.acct_id AS acct_no,
wkr.dealer_cd,
wkr.work_ord_create_dt AS work_ord_create_dt,
wkr.work_ord_completion_dt AS work_ord_comp_dt,
work_ord_completion_tm AS work_ord_comp_tm,
wkr.work_ord_status,
work_ord_create_id,
work_ord_type,
component_id_dv,
com.component_id,
com.active_dt,
com.inactive_dt,
com.bill_product_action,
pkg.package_id,
prod_grp_lvl_3 AS plan_type
FROM
ipviews_srd.OO_AWKORD wkr -- select * from ipviews_srd.OO_AWKORD where work_ord_no = 'UDQ3355001'

LEFT OUTER JOIN ipviews_srd.oo_btxnpa pkg
ON wkr.work_ord_no = pkg.work_ord_no
LEFT OUTER JOIN ipviews_srd.oo_btxncm com
ON pkg.int_pack_inst = com.int_pack_inst
LEFT OUTER JOIN
ipviews_srd.ar_component_definition_values ar
ON com.component_id = ar.component_id
AND ar.close_dt = '2899-12-31'
inner join  ipcfg.cfg_rate_plan_ref ref
ON com.component_id =  ref.rate_plan_cd
WHERE  wkr.work_ord_status IN ('4')
AND com.bill_product_action IN ('1','2')
and rate_plan_lvl_3 = 'Pacman'
and rate_plan_lvl_2 = 'TBC bolton'
and work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param)
and com.active_dt is not null
) with data primary index (acct_no)
on commit preserve rows;


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn
select 
'Gross Adds' as status_cd
,null as svc_no
,w.acct_no
,a.bill_to_nm 
,cast(w.component_id as varchar(15)) as bolton_rate_plan_cd
,w.component_id_dv as bolton_rate_plan_ds
,cast(plan_component_id as varchar(15))
,plan_component_desc
,null as ext_id_type
,null as svc_type
,'SMB' as cfu
,null as vip_code
,null as owning_cost_ctr
,null as owning_cost_ctr_dv
,null as rental_tariff_group
, null as work_ord_create_id
,null as rep_cd
,w.dealer_cd
,post_cd
,work_ord_no
,null as ext_ord_ref_no
,null as svc_inst_id
,null as subscr_no
,active_dt as bolton_active_dt
,null as bolton_inactive_dt
,work_ord_comp_dt
, work_ord_comp_tm
, work_ord_create_dt
,null as work_ord_create_tm
,work_ord_type
,bill_product_action
,null as svc_action_type
, null as sales_chanl_id
, null as wrkjn_rep_id
, null as retl_store
,null as disconnect_reason
,null as disconnect_cd
,null as original_drl_cd
,null as original_rep_id
,null as no_of_svc_x_acct
,current_date as load_date
,'oo_awkord' src_tbl
from 
tbc_wo w
left outer join 
spcomm.f_tbc_acct_plans a 
on w.acct_no = a.acct_no
and w.active_dt between a.plan_active_dt and a.plan_inactive_dt
and a.plan_type = 'Voice Plan'
QUALIFY RANK() OVER (
PARTITION BY w.acct_no, w.component_id , w.work_ord_no
                            ORDER BY  a.plan_inactive_dt DESC) = 1     ; 


.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
--- churns -- 


create volatile table tbc_canx as (
select 
wkr.work_ord_no,
wkr.acct_id AS acct_no,
wkr.dealer_cd,
wkr.work_ord_create_dt AS work_ord_create_dt,
wkr.work_ord_completion_dt AS work_ord_comp_dt,
work_ord_completion_tm AS work_ord_comp_tm,
wkr.work_ord_status,
work_ord_create_id,
work_ord_type,
component_id_dv,
com.component_id,
com.active_dt,
com.inactive_dt,
com.bill_product_action,
pkg.package_id,
prod_grp_lvl_3 AS plan_type
FROM
ipviews_srd.OO_AWKORD wkr -- select * from ipviews_srd.OO_AWKORD where work_ord_no = 'UDQ3355001'

LEFT OUTER JOIN ipviews_srd.oo_btxnpa pkg
ON wkr.work_ord_no = pkg.work_ord_no
LEFT OUTER JOIN ipviews_srd.oo_btxncm com
ON pkg.int_pack_inst = com.int_pack_inst
LEFT OUTER JOIN
ipviews_srd.ar_component_definition_values ar
ON com.component_id = ar.component_id
AND ar.close_dt = '2899-12-31'
inner join  ipcfg.cfg_rate_plan_ref ref
ON com.component_id =  ref.rate_plan_cd
WHERE  wkr.work_ord_status IN ('4')
AND com.bill_product_action IN ('3','4')
and rate_plan_lvl_3 = 'Pacman'
and rate_plan_lvl_2 = 'TBC bolton'
and work_ord_completion_dt between (select start_dt from dt_param) and (select end_dt from dt_param)
and com.inactive_dt is not null
) with data primary index (acct_no)
on commit preserve rows;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

insert into spcomm.f_bolton_add_churn
select 
'Churns' as status_cd
,null as svc_no
,w.acct_no
,a.bill_to_nm 
,cast(w.component_id as varchar(15)) as bolton_rate_plan_cd
,w.component_id_dv as bolton_rate_plan_ds
,cast(plan_component_id as varchar(15))
,plan_component_desc
,null as ext_id_type
,null as svc_type
,'SMB' as cfu
,null as vip_code
,null as owning_cost_ctr
,null as owning_cost_ctr_dv
,null as rental_tariff_group
, null as work_ord_create_id
,null as rep_cd
,w.dealer_cd
,post_cd
,work_ord_no
,null as ext_ord_ref_no
,null as svc_inst_id
,null as subscr_no
,active_dt as bolton_active_dt
,null as bolton_inactive_dt
,work_ord_comp_dt
, work_ord_comp_tm
, work_ord_create_dt
,null as work_ord_create_tm
,work_ord_type
,bill_product_action
,null as svc_action_type
, null as sales_chanl_id
, null as wrkjn_rep_id
, null as retl_store
,null as disconnect_reason
,null as disconnect_cd
,null as original_drl_cd
,null as original_rep_id
,null as no_of_svc_x_acct
,current_date as load_date
,'oo_awkord' src_tbl
from 
tbc_canx w
left outer join 
spcomm.f_tbc_acct_plans a 
on w.acct_no = a.acct_no
and w.active_dt between a.plan_active_dt and a.plan_inactive_dt
and a.plan_type = 'Voice Plan'
QUALIFY RANK() OVER (
PARTITION BY w.acct_no, w.component_id , w.work_ord_no
                            ORDER BY  a.plan_inactive_dt DESC) = 1     ; 
                            
.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE                            
                            
--- fetch market segment --- 
-- mobile --- 
create volatile table acct as (
select acct_no  from spcomm.f_bolton_add_churn where no_of_svc_x_acct is null 
and  work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)         
and src_tbl in (
'Opom Extract',
'adac_fbb',
'oo_awkord',
'OTT Trans')
) with data primary index (acct_no)
on commit preserve rows;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

CREATE VOLATILE TABLE mkt_seg
AS
(
SEL acct_id, COUNT(distinct svc_inst_id) AS no_of_svc_x_acct
from
(select acct_id, inst.svc_inst_id
FROM ipviews_srd.oo_svcins inst
inner JOIN ipviews_srd.oo_svciex s
ON s.svc_inst_id = inst.svc_inst_id
WHERE inst.acct_id IN (SEL * FROM acct  GROUP BY 1)
AND s.close_dt = '2899-12-31' AND inst.close_dt = '2899-12-31' AND s.ext_id_type = '20'
AND (s.end_dt IS NULL or s.end_dt <= (select end_dt from dt_param))
AND (inst.end_dt IS NULL or inst.end_dt <= (select end_dt from dt_param))
QUALIFY RANK() OVER (
PARTITION BY acct_id, inst.svc_inst_id
                            ORDER BY  coalesce(inst.end_dt, date '2899-12-31') DESC, coalesce(s.end_dt, date '2899-12-31') DESC) = 1    
) x
group by 1
) WITH DATA primary index (acct_id)
ON COMMIT PRESERVE ROWS
;         

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

update
a
from
spcomm.f_bolton_add_churn a,
mkt_seg b
set no_of_svc_x_acct = b.no_of_svc_x_acct
where
a.work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)     
and a.acct_no = b.acct_id
and a.ext_id_type = '20'
and a.no_of_svc_x_acct is null;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE



collect stats on spcomm.f_bolton_add_churn column (svc_no);
collect stats on spcomm.f_bolton_add_churn column (bolton_rate_plan_cd);
collect stats on spcomm.f_bolton_add_churn column (basic_rate_plan_cd);
collect stats on spcomm.f_bolton_add_churn column (work_ord_completion_dt);


.quit errorcode
.logoff;





 

