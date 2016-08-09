.RUN FILE = E:\SMB\report_scripts\template\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

/*  Bolton Adds 
/* Setting date range-- 15 days sliding window */
/*
-- 26-04-2012 SP - modify sliding window
-- CMBS rate plan
--- 03-05-2012 SP -- modify MeTV to pull in Activations and Disconnects 
--- 04-07-2012 SP -- apply ranking of ipivew acct
--- 20-07-2012 SP -- fixed MeTV churns which was loading from opom and OTT table at the same time
--  11-07-2014 : MC
--   Add package group (24,25,26,27) for my plan plus
package_group	display_value
3	ULL Tel
4	ULL DSL
5	Basic Access
6	Basic Access Fleet
22	NBN Broadband plans
22	NBN FBB plans
24	Basic Access
25	Basic Access Fleet
26	AR Shared Bolt-ons
27	Basic Access
*/

CREATE VOLATILE TABLE LOG_MAX AS
(
SEL
CAST(TRIM(REPORT_ID)||TRIM(NEW_ID) AS INTEGER) AS INSERT_NUM,
REPORT_ID
FROM
(SEL 
COUNT(*)  AS NEW_ID,
10272 AS REPORT_ID
FROM IPSHARE_PD.GF_TEMP_LOG
WHERE REPORT_ID = 10272) A
)WITH DATA ON COMMIT PRESERVE ROWS;


INSERT INTO  IPSHARE_PD.GF_TEMP_LOG
SELECT
LOG_MAX.INSERT_NUM,  
LOG_MAX.REPORT_ID, 
CURRENT_DATE AS START_DATE,
CURRENT_TIME AS START_TIME,
NULL AS END_DATE,
NULL AS END_TIME,
'RUNNING' AS STATUS;


CREATE VOLATILE TABLE dt_param
AS
( SELECT
 (CURRENT_DATE - 12) AS start_dt,
CURRENT_DATE-2 AS end_dt
)
WITH  DATA
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF
--  select min(work_ord_completion_dt) from spcomm.f_bolton_add_churn

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
reps_id AS wrkjn_rep_id,
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
          AND s.ext_id_type IN ('20','601','15')     --601 DSLD; 15 - ULL
          AND s.start_dt <= ord.work_ord_completion_dt
          AND work_ord_completion_dt BETWEEN s.open_dt-2 AND s.close_dt
          AND work_ord_completion_dt BETWEEN s.start_dt AND COALESCE(s.end_dt, DATE '2899-12-31')
 	INNER JOIN spcomm.bolton_rp_ref r
          ON cmp.component_id =  r.rate_plan_cd
          AND r.bolton_in = '1'
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
                            ORDER BY  work_ord_completion_dt DESC, work_ord_completion_tm DESC,  svc.close_dt DESC, s.open_dt DESC, s.start_dt DESC, sj.open_dt ASC,ordjn.open_dt ASC) = 1 
) WITH DATA
PRIMARY INDEX ( svc_no ,ACCT_ID ,WORK_ORD_COMPLETION_DT , COMPONENT_ID ,cmp_active_dt )
ON COMMIT PRESERVE ROWS 
;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

/*   select work_ord_no, component_id, count(*)  from wo_ga group by 1,2 having count(*) > 1

select * from wo_ga where work_ord_no = 'UGG9793001'
select * from 
ipviews_srd.oo_btxnpa pkg
	INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
          left outer join ipviews_srd.AR_COMPONENT_DEFINITION_VALUES a
          on cmp.component_id = a.component_id
where work_ord_no = 'UGG9793001'   */
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


.IF ERRORLEVEL > 0 THEN GOTO ERRIF
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CFU derivation
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE MULTISET TABLE vt_ga AS (
SELECT 
svc_no,
acct_id AS acct_no,
bill_lname AS customer_name,
component_id AS bolton_rate_plan,
rate_plan_ds AS bolton_rate_plan_ds,
ext_id_type,
svc_type,
sc.cfu,
vip_code,
owning_cost_ctr,
owning_cost_ctr_dv,
rt.decoded_value AS rental_tariff_group,
work_ord_create_id,
rep_id,
dealer_cd,
bill_zip AS post_cd,
work_ord_no,
ext_ord_ref_no,
svc_inst_id,
cmp_active_dt AS bolton_active_dt,
cmp_inactive_dt AS bolton_inactive_dt,
work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_tm,
work_ord_type,
cmp_bill_product_act_def AS bill_product_action,
mc.decoded_value AS svc_action_type,
sales_chanl_id,
wrkjn_rep_id,
retl_store
FROM
ga_vip
LEFT OUTER JOIN 
ipviews_md.md_sales_class_ref sc
ON ga_vip.vip_code = sc.sales_class_ref_cd
AND sc.close_dt = '2899-12-31'
LEFT OUTER JOIN ipviews_srd.oo_reftmc mc
ON ga_vip.svc_action_type = mc.mstr_cd
AND mc.close_dt = DATE '2899-12-31'  
AND mc.mstr_cd_key_type = 'svc_action_ty'
LEFT OUTER JOIN ipviews_srd.oo_reftmc rt
ON ga_vip.rental_tariff_grp = rt.mstr_cd
AND rt.mstr_cd_key_type = 'RENTAL_TARIFF_GRP'
AND rt.close_dt = DATE '2899-12-31'  
) WITH DATA PRIMARY INDEX (svc_no, work_ord_completion_dt)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- basic rate plan derivation for new adds
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- fetch subscr no

CREATE VOLATILE TABLE ga_subscr AS (
SELECT 
vt_ga.svc_no, work_ord_no, s200.subscr_no, work_ord_completion_dt
FROM vt_ga
INNER JOIN ipviews_srd.ar_s200 s200
ON vt_ga.svc_no = s200.external_id 
AND vt_ga.ext_id_type = s200.external_id_type
AND work_ord_completion_dt BETWEEN CAST(s200.active_date AS DATE) -2 AND COALESCE(s200.inactive_date, DATE '2899-12-31')
AND s200.record_type <> '40'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  s200.close_dt DESC, s200.active_date DESC  ) = 1 
) WITH DATA PRIMARY INDEX (svc_no, subscr_no)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF
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

-- 11-07-2014 : Add package_group (24,25,26,27)
AND pac.package_group IN (3,4,5,6,22,24,25,26,27)

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


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- consolidating  adds

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE MULTISET TABLE vt_ga_fin AS (
SELECT 
vt_ga.svc_no,
vt_ga.acct_no,
customer_name,
bolton_rate_plan AS bolton_rate_plan_cd,
bolton_rate_plan_ds,
CAST(TRIM(basic_rate_plan_cd) AS VARCHAR(15)) AS basic_rate_plan_cd,
component_id_dv AS basic_plan_ds,
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
FROM
vt_ga
LEFT OUTER JOIN 
ga_basic
ON vt_ga.svc_no = ga_basic.svc_no
AND vt_ga.work_ord_no = ga_basic.work_ord_no
) WITH DATA PRIMARY INDEX (svc_no)
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*  bolton churns */

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
reps_id AS wrkjn_rep_id,
retl_store
FROM      ipviews_srd.oo_svcins svc
    INNER JOIN ipviews_srd.oo_wrkord ord
		ON  svc.svc_inst_id = ord.svc_inst_id
         AND   ord.work_ord_close_dt BETWEEN svc.open_dt-2 AND svc.close_dt
    INNER JOIN ipviews_srd.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
     INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
     INNER JOIN ipviews_srd.oo_svciex s   -- select * from ipviews_srd.oo_svciex where svc_inst_id = '100002407653'
          ON  s.svc_inst_id = ord.svc_inst_id
          AND s.ext_id_type IN ('20','601','15')     --601 DSLD; 15 - ULL
          AND s.start_dt <= ord.work_ord_completion_dt
          AND work_ord_completion_dt BETWEEN s.open_dt-2 AND s.close_dt
          AND work_ord_completion_dt BETWEEN s.start_dt AND COALESCE(s.end_dt, DATE '2899-12-31')
    INNER JOIN spcomm.bolton_rp_ref r
          ON cmp.component_id =  r.rate_plan_cd
      AND r.bolton_in = '1'
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
                            ORDER BY  work_ord_completion_dt DESC, work_ord_completion_tm DESC,  svc.close_dt DESC, s.close_dt DESC,COALESCE(s.end_dt, DATE '2899-12-31') DESC , s.start_dt DESC, sj.close_dt ASC, ordjn.close_dt DESC, cmp_inactive_dt ASC ) = 1 
) WITH DATA PRIMARY INDEX (svc_no, acct_id, component_id,cmp_active_dt,work_ord_completion_dt)
ON COMMIT PRESERVE ROWS ;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- vip code

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


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

-- consolidate churn

CREATE VOLATILE MULTISET TABLE vt_churn AS (
SELECT 
svc_no,
acct_id AS acct_no,
bill_lname AS customer_name,
component_id AS bolton_rate_plan,
rate_plan_ds AS bolton_rate_plan_ds,
ext_id_type,
svc_type,
sc.cfu,
vip_code,
owning_cost_ctr,
owning_cost_ctr_dv,
rt.decoded_value AS rental_tariff_group,
work_ord_create_id,
rep_id,
dealer_cd,
bill_zip AS post_cd,
work_ord_no,
ext_ord_ref_no,
svc_inst_id,
cmp_active_dt AS bolton_active_dt,
cmp_inactive_dt AS bolton_inactive_dt,
work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_tm,
work_ord_type,
cmp_bill_product_act_def AS bill_product_action,
mc.decoded_value AS svc_action_type,
sales_chanl_id,
wrkjn_rep_id,
retl_store,
disconnect_reason,
disconnect_cd
FROM
churn_vip
LEFT OUTER JOIN 
ipviews_md.md_sales_class_ref sc
ON churn_vip.vip_code = sc.sales_class_ref_cd
AND sc.close_dt = '2899-12-31'
LEFT OUTER JOIN ipviews_srd.oo_reftmc mc
ON churn_vip.svc_action_type = mc.mstr_cd
AND mc.close_dt = DATE '2899-12-31'  
AND mc.mstr_cd_key_type = 'svc_action_ty'
LEFT OUTER JOIN ipviews_srd.oo_reftmc rt
ON churn_vip.rental_tariff_grp = rt.mstr_cd
AND rt.mstr_cd_key_type = 'RENTAL_TARIFF_GRP'
AND rt.close_dt = DATE '2899-12-31'  
) WITH DATA PRIMARY INDEX (svc_no, work_ord_completion_dt)
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- basic rate plan derivation for churns
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- fetch subscr no


CREATE VOLATILE TABLE churn_subscr AS (
SELECT 
vt_churn.svc_no, work_ord_no, s200.subscr_no, work_ord_completion_dt
FROM vt_churn
INNER JOIN ipviews_srd.ar_s200 s200
ON vt_churn.svc_no = s200.external_id 
AND vt_churn.ext_id_type = s200.external_id_type
AND work_ord_completion_dt BETWEEN s200.open_dt -3  AND  s200.close_dt
AND work_ord_completion_dt BETWEEN s200.active_date  AND  COALESCE(s200.inactive_date, DATE '2899-12-31') +3
AND s200.record_type <> '40'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  s200.close_dt DESC, s200.inactive_date DESC  ) = 1 
) WITH DATA PRIMARY INDEX (svc_no, subscr_no)
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

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

-- 11-07-2014 : Add package_group (24,25,26,27)
AND pac.package_group IN (3,4,5,6,22,24,25,26,27)

AND pac.close_dt = '2899-12-31'
INNER JOIN
ipviews_srd.AR_COMPONENT_DEFINITION_VALUES ar
ON p200.component_id =ar.component_id
AND ar.close_dt = '2899-12-31'
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_completion_dt,work_ord_no
                            ORDER BY  p200.product_start DESC, COALESCE(p200.product_stop, DATE '2899-12-31') DESC, p200.close_dt DESC ) = 1    
) WITH DATA PRIMARY INDEX (svc_no, subscr_no, basic_rate_plan_cd)
ON COMMIT PRESERVE ROWS ;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---consolidate churns 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE MULTISET TABLE vt_churn_fin AS (
SELECT 
'Churns' AS status_cd,
vt_churn.svc_no,
vt_churn.acct_no,
customer_name,
bolton_rate_plan AS bolton_rate_plan_cd,
bolton_rate_plan_ds,
CAST(TRIM(basic_rate_plan_cd) AS VARCHAR(15)) AS basic_rate_plan_cd,
component_id_dv AS basic_plan_ds,
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
FROM
vt_churn
LEFT OUTER JOIN 
churn_basic
ON vt_churn.svc_no = churn_basic.svc_no
AND vt_churn.work_ord_no = churn_basic.work_ord_no
) WITH DATA PRIMARY INDEX (svc_no)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF
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
reps_id AS wrkjn_rep_id
FROM    
vt_churn_fin vt
	INNER JOIN
   		 ipviews_srd.oo_svcins svc   ---select * from  ipviews_srd.oo_svcins where svc_inst_id = '100008626232'
		ON vt.svc_inst_id = svc.svc_inst_id
	INNER JOIN  ipviews_srd.oo_wrkord ord
          ON  svc.svc_inst_id = ord.svc_inst_id
		 --AND ord.work_ord_completion_dt <= vt.work_ord_completion_dt
	INNER JOIN ipviews_srd.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
	INNER JOIN ipviews_srd.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
LEFT OUTER  JOIN IPVIEWS_SRD.oo_svcijn sj
          ON  sj.svc_inst_id = ord.svc_inst_id 
	 AND ord.work_ord_completion_dt BETWEEN sj.open_dt-1 AND sj.close_dt
	LEFT OUTER JOIN  ipviews_srd.OO_WRKOJN   ordjn  --- select * from ipviews_srd.OO_WRKOJN where work_ord_no = 'QRD0967003'
		ON ord.WORK_ORD_NO = ordjn.WORK_ORD_NO
		AND ord.work_ord_completion_dt BETWEEN ordjn.open_dt-2 AND ordjn.close_dt
WHERE     ord.work_ord_status IN (4) -- completed
AND cmp.bill_product_action IN ('1','2') -- 1 provide, 2 - change
AND ord.work_ord_type IN ('1','2') -- 1 provide, 2 - change
AND ord.svc_action_type NOT IN ('40','1') -- 40 migraiton, 1 - SI transfer
AND cmp.inactive_dt IS NULL
AND cmp_active_dt = vt.bolton_active_dt
AND cmp.component_id = vt.bolton_rate_plan_cd
QUALIFY RANK() OVER (
PARTITION BY component_id,vt.svc_inst_id
                            ORDER BY  ord.work_ord_completion_dt DESC, ord.work_ord_completion_tm DESC,  ordjn.open_dt ASC,ordjn.open_dt ASC) = 1 
) WITH DATA
PRIMARY INDEX ( svc_inst_id , COMPONENT_ID ,cmp_active_dt )
ON COMMIT PRESERVE ROWS ;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

CREATE VOLATILE TABLE vt_churn_fin_2 AS (
SELECT 
f.*,
o.dealer_cd AS orginal_dlr_cd,
o.wrkjn_rep_id AS orginal_rep_id
 FROM vt_churn_fin f
LEFT OUTER JOIN 
orig_wo o
ON f.svc_inst_id = o.svc_inst_id
AND f.bolton_rate_plan_cd = o.component_id
AND f.bolton_active_dt = o.cmp_active_dt
) WITH DATA PRIMARY INDEX (svc_no)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

DELETE FROM spcomm.f_bolton_add_churn 
WHERE work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)
AND src_tbl = 'Opom Extract';

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn 
SELECT a.*, NULL AS no_of_svc_x_acct, CURRENT_DATE AS load_date, 'Opom Extract' AS src_tbl  FROM vt_churn_fin_2 a 
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_no, work_ord_completion_dt, bolton_rate_plan_cd
                            ORDER BY  bolton_inactive_dt DESC) = 1 
;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn
SELECT 'Gross Adds', a.*, NULL AS disconnect_reason, NULL AS disconnect_cd, NULL AS orginal_dlr_cd, NULL AS orginal_rep_id, NULL AS no_of_svc_x_acct, CURRENT_DATE AS load_date, 'Opom Extract' AS src_tbl FROM vt_ga_fin a 
QUALIFY RANK() OVER (
PARTITION BY svc_no, work_ord_no, work_ord_completion_dt, bolton_rate_plan_cd
                            ORDER BY  bolton_active_dt DESC) = 1 ;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- METV ---
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE VOLATILE MULTISET TABLE metv AS (
SELECT
CASE WHEN action_type = 'DISCONNECTION' THEN 'Churns'
			WHEN action_type = 'Activation' THEN 'Gross Adds'
			END AS event
,service_id
,account_number
,CAST(NULL AS VARCHAR(50)) AS customer_name
,CASE WHEN service_type = 'ONC' THEN CMBS_rate_plan_id ELSE opom_component_id END  AS bolton_rate_plan_cd
,product_code AS bolton_rate_plan_ds
,CAST(CASE WHEN POSITION('_' IN sys_rate_plan_cd) > 0    
                        THEN SUBSTRING(sys_rate_plan_cd FROM POSITION('_' IN sys_rate_plan_cd)+1 FOR POSITION('_' IN SUBSTRING(sys_rate_plan_cd
                           FROM POSITION('_' IN sys_rate_plan_cd)+1))-1)
                    ELSE sys_rate_plan_cd
                  END AS VARCHAR(32))  AS basic_rate_plan_cd
,COALESCE(plan_name, sys_rate_plan_nm) AS basic_plan_ds
,NULL AS ext_id_type
,NULL AS svc_type
,cfu
,NULL AS vip_code
,NULL AS owning_cost_ctr
,NULL AS owning_cost_ctr_dv
,NULL AS rental_tariff_group
,NULL AS work_ord_create_id
,CAST(NULL AS VARCHAR(15)) AS rep_id
,CAST(NULL AS VARCHAR(15)) AS dealer_id
,addr_post_cd AS post_cd
,work_ord_no
,sos_order_no
,NULL AS svc_inst_id
,NULL AS subscr_no
,transaction_date AS bolton_active_dt
,NULL AS bolton_inactive_dt
,transaction_date AS work_ord_completion_dt
,NULL AS work_ord_completion_tm
,NULL AS work_ord_create_dt
,NULL AS work_ord_create_tm
,NULL AS work_ord_type
,NULL AS bill_product_action
,NULL AS svc_action_type
,opom_sales_chanl_id AS sales_chanl_id
,NULL AS wrjn_rep_id
,NULL AS retl_store
,NULL AS disconnect_reason
,NULL AS disconnect_cd
FROM IPSHARE.OTT_AT_TRANSACTION   --- select * from ipshare.ott_at_transaction where service_id    ='sagrawal' = 'buzzabuzza'
WHERE transaction_date BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND action_type IN (
'ACTIVATION',
'DISCONNECTION')
QUALIFY RANK() OVER (
PARTITION BY event, service_id, bolton_rate_plan_cd, transaction_date   --- need to qualify due to source data having two dealers on some records!!
                            ORDER BY dealer_id DESC, rep_id DESC ) = 1  
) WITH DATA PRIMARY INDEX (service_id)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

--- attached the dealer and rep id that placed the order for the activation


CREATE VOLATILE MULTISET TABLE metv_output AS (
SELECT 
metv.*, 
ott.dealer_id AS new_dealer_id,
ott.rep_id AS new_rep_id,
work_ord_completion_dt-ott.transaction_date AS days_diff
FROM metv
LEFT OUTER JOIN 
IPSHARE.OTT_AT_TRANSACTION ott
ON metv.service_id = ott.service_id
AND ott.action_type = 'Order'
AND ott.dealer_id <> '0'
AND ott.transaction_date <= work_ord_completion_dt
QUALIFY RANK() OVER (
PARTITION BY event, metv.service_id, metv.account_number, bolton_rate_plan_cd, metv.work_ord_no, work_ord_completion_dt
                            ORDER BY  days_diff ASC) = 1    
) WITH DATA PRIMARY INDEX (service_id)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

-- fetch customer_name

UPDATE a
FROM metv_output a,
(SELECT acct_no, acct_name FROM ipviews.acct 
WHERE close_dt = '2899-12-31'
QUALIFY RANK() OVER (
PARTITION BY acct_name
                            ORDER BY acct_no DESC, open_dt DESC ) = 1  
) p
SET customer_name = acct_name
WHERE account_number = p.acct_no
;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

-- delete statement

DELETE FROM spcomm.f_bolton_add_churn 
WHERE work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND src_tbl = 'OTT Trans';

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

--- insert all except order

INSERT INTO
spcomm.f_bolton_add_churn
SELECT 
event
,service_id
,account_number
,customer_name
,bolton_rate_plan_cd
,bolton_rate_plan_ds
,basic_rate_plan_cd
,basic_plan_ds
,ext_id_type
,svc_type
,cfu
,vip_code
,owning_cost_ctr
,owning_cost_ctr_dv
,rental_tariff_group
,work_ord_create_id
,CASE WHEN event = 'Gross Adds' THEN new_dealer_id ELSE rep_id END
,CASE WHEN event = 'Gross Adds' THEN new_rep_id  ELSE dealer_id END
,post_cd
,work_ord_no
,sos_order_no
,svc_inst_id
,subscr_no
,bolton_active_dt
,bolton_inactive_dt
,work_ord_completion_dt
,work_ord_completion_tm
,NULL AS work_ord_create_dt
,NULL AS work_ord_create_dt_tm
,NULL AS work_ord_type
,NULL AS bill_product_action
,NULL AS svc_action_type
,NULL AS sales_chanl_id
,NULL AS wrkjn_rep_id
,NULL AS retl_store
,NULL AS disconnect_reason
,NULL AS disconnect_cd
,CASE WHEN event = 'Churns' THEN new_dealer_id ELSE NULL END AS original_dlr_cd
,CASE WHEN event = 'Churns' THEN new_rep_id ELSE NULL END AS original_rep_id
,NULL AS no_of_svc_x_acct
,CURRENT_DATE AS load_date
,'OTT Trans' AS src_tbl
FROM metv_output;



.IF ERRORLEVEL > 0 THEN GOTO ERRIF                            




----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- FBB bolton speed pack  ---
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

DELETE FROM spcomm.f_bolton_add_churn
WHERE src_tbl = 'adac_fbb'
AND work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn
SELECT
CASE WHEN svc_stat_cd = 'A' THEN 'Gross Adds'
			WHEN svc_stat_cd ='C' THEN 'Churns'
			ELSE svc_stat_cd END AS status_cd
,svc_no
,acct_no
,bill_to_acct_nm
,ref.rate_plan_cd AS bolton_rate_plan
,ref.rate_plan_ds AS bolton_rate_plan_ds
,basic_accs_rate_plan_cd
,component_id_dv AS basic_rate_plan_ds
,NULL AS ext_id
,NULL AS svc_type
,cfu 
,NULL AS vip_code
,NULL AS owning_cost_ctr
,NULL AS owning_cost_ctr_dv
, NULL AS rental_tariff_group
, NULL AS work_ord_create_id
,rep_cd AS rep_id
,dlr_cd
,cust_post_cd
,ref_no AS work_ord_no
,NULL AS ext_ord_ref_no
,NULL AS svc_inst_id
,NULL AS subscr_no
,day_dt AS bolton_active_dt
,NULL AS bolton_inactive_dt
,day_dt AS work_ord_completion_dt
,NULL AS work_ord_completion_tm
,NULL AS work_ord_create_dt
,NULL AS work_ord_create_tm
,NULL AS work_ord_type
,NULL AS bill_product_action
,NULL AS svc_action_type
,NULL AS sales_chanl_id
,NULL AS wrkjn_rep_id
,NULL AS retl_store
,rsn_cd AS disconnect_reason
,NULL AS disconnect_cd
,NULL AS original_dlr_cd
,NULL AS original_rep_id
,NULL AS no_of_svc_x_acct
,CURRENT_DATE AS load_date
,'adac_fbb' AS src_tbl
FROM spcomm.all_daily_add_churn ac
LEFT OUTER JOIN spcomm.cfg_sov_ref ref
	ON ac.contrct_nm = ref.rate_plan_ds
	AND ref.end_dt = '2899-12-31'
LEFT OUTER JOIN ipviews_srd.AR_COMPONENT_DEFINITION_VALUES com
	ON ac.basic_accs_rate_plan_cd = com.component_id
	AND com.close_dt = '2899-12-31'
WHERE src_tbl IN ('fixed_fbb_adds_stg','fixed_fbb_canx_stg')
AND day_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND ref.rate_plan_cd IS NOT NULL;


--- mobile data boltons  ---


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

DELETE FROM spcomm.f_bolton_add_churn
WHERE src_tbl = 'adac_mobdata'
AND work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn
SELECT 
CASE WHEN service_event_grp = 'Gross Adds' THEN 'Gross Adds'
		  WHEN service_event_grp = 'Churns' THEN 'Churns'
		  ELSE NULL END AS status_cd
,svc_no
,acct_no
,bill_to_acct_nm
,bolton_rate_plan_cd
,r.rate_plan_ds AS bolton_rate_plan_ds
,CASE 
                    WHEN POSITION('_' IN basic_accs_rate_plan_cd) > 0    
                    THEN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1 FOR POSITION('_' IN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1))-1)
                    ELSE basic_accs_rate_plan_cd
                  END   AS basic_rate_plan_cd
,b.rate_plan_ds AS basic_plan_ds
,NULL AS ext_id_type
,NULL AS svc_type
,cfu
,NULL AS vip_code
,NULL AS owning_cost_ctr
,NULL AS owning_cost_ctr_dv
,NULL AS rental_tariff_group
, NULL AS work_ord_create_id
,rep_cd
,dlr_cd
,cust_post_cd
,NULL AS work_ord_no
,NULL AS ext_ord_ref_no
,NULL AS svc_inst_id
,NULL AS subscr_no
,CASE WHEN service_event_grp = 'Gross Adds' THEN day_dt ELSE NULL END AS bolton_active_dt
,CASE WHEN service_event_grp = 'Churns' THEN day_dt ELSE NULL END  AS bolton_inactive_dt
,day_dt AS work_ord_completion_dt
, NULL AS work_ord_completion_tm
, NULL AS work_ord_create_dt
, NULL AS work_ord_create_tm
,NULL AS work_ord_type
, NULL AS bill_product_action
,NULL AS svc_action_type
, NULL AS sales_chanl_id
, NULL AS wrkjn_rep_id
, NULL AS retl_store
,NULL AS disconnect_reason
,NULL AS disconnect_cd
,NULL AS original_drl_cd
,NULL AS original_rep_id
,no_of_svc_x_acct
,CURRENT_DATE AS load_date
,'adac_mobdata' src_tbl
FROM spcomm.all_daily_add_churn ac
INNER JOIN ipcfg.cfg_rate_plan_ref r
ON ac.bolton_rate_plan_cd = r.rate_plan_cd
INNER JOIN bpviews_app.L_SERVICE_EVENT se
ON ac.svc_stat_cd = service_event_cd
LEFT OUTER JOIN ipcfg.cfg_rate_plan_ref b 
ON basic_rate_plan_cd = b.rate_plan_cd
WHERE ac.src_tbl = 'mob_mobile_data'
AND ac.day_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND se.service_event_grp IN ('Gross Adds','Churns')
AND r.prod_grp_lvl_1 = 'Mobile Internet Data Bolt On'
AND ac.cfu IN ('SMB','Consumer')
AND status_cd IS NOT NULL;



--- blackberry boltons  ---


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

DELETE FROM spcomm.f_bolton_add_churn
WHERE src_tbl = 'adac_mob_bb'
AND work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);

--.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn
SELECT 
CASE WHEN service_event_grp = 'Gross Adds' THEN 'Gross Adds'
		  WHEN service_event_grp = 'Churns' THEN 'Churns'
		  ELSE NULL END AS status_cd
,svc_no
,acct_no
,bill_to_acct_nm
,bolton_rate_plan_cd
,r.rate_plan_ds AS bolton_rate_plan_ds
,CASE 
                    WHEN POSITION('_' IN basic_accs_rate_plan_cd) > 0    
                    THEN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1 FOR POSITION('_' IN SUBSTRING(basic_accs_rate_plan_cd FROM POSITION('_' IN basic_accs_rate_plan_cd)+1))-1)
                    ELSE basic_accs_rate_plan_cd
                  END   AS basic_rate_plan_cd
,b.rate_plan_ds AS basic_plan_ds
,NULL AS ext_id_type
,NULL AS svc_type
,cfu
,NULL AS vip_code
,NULL AS owning_cost_ctr
,NULL AS owning_cost_ctr_dv
,NULL AS rental_tariff_group
, NULL AS work_ord_create_id
,rep_cd
,dlr_cd
,cust_post_cd
,NULL AS work_ord_no
,NULL AS ext_ord_ref_no
,NULL AS svc_inst_id
,NULL AS subscr_no
,day_dt AS bolton_active_dt
,day_dt AS bolton_inactive_dt
,day_dt AS work_ord_completion_dt
, NULL AS work_ord_completion_tm
, NULL AS work_ord_create_dt
, NULL AS work_ord_create_tm
,NULL AS work_ord_type
, NULL AS bill_product_action
,NULL AS svc_action_type
, NULL AS sales_chanl_id
, NULL AS wrkjn_rep_id
, NULL AS retl_store
,NULL AS disconnect_reason
,NULL AS disconnect_cd
,NULL AS original_drl_cd
,NULL AS original_rep_id
,no_of_svc_x_acct
,CURRENT_DATE AS load_date
,'adac_mob_bb' src_tbl
FROM spcomm.all_daily_add_churn ac
INNER JOIN ipcfg.cfg_rate_plan_ref r
ON ac.bolton_rate_plan_cd = r.rate_plan_cd
INNER JOIN bpviews_app.L_SERVICE_EVENT se
ON ac.svc_stat_cd = service_event_cd
LEFT OUTER JOIN ipcfg.cfg_rate_plan_ref b
ON basic_rate_plan_cd = b.rate_plan_cd
WHERE src_tbl = 'mob_blackberry'
AND day_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND service_event_grp IN ('Gross Adds','Churns')
AND r.commissionable_in = 'Y'
AND r.bb_cnt_in = '1'
AND r.bolton_in ='1'
AND r.prod_grp_lvl_1 = 'Blackberry'
AND ac.cfu IN ('SMB','Consumer')
;

-- account level boltons -- 


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

DELETE FROM spcomm.f_bolton_add_churn
WHERE src_tbl = 'oo_awkord'
AND work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);


.IF ERRORLEVEL > 0 THEN GOTO ERRIF


CREATE VOLATILE TABLE tbc_wo AS (
SELECT 
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
INNER JOIN  ipcfg.cfg_rate_plan_ref ref
ON com.component_id =  ref.rate_plan_cd
WHERE  wkr.work_ord_status IN ('4')
AND com.bill_product_action IN ('1','2')
AND rate_plan_lvl_3 = 'Pacman'
AND rate_plan_lvl_2 = 'TBC bolton'
AND work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND com.active_dt IS NOT NULL
) WITH DATA PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn
SELECT 
'Gross Adds' AS status_cd
,NULL AS svc_no
,w.acct_no
,a.bill_to_nm 
,CAST(w.component_id AS VARCHAR(15)) AS bolton_rate_plan_cd
,w.component_id_dv AS bolton_rate_plan_ds
,CAST(plan_component_id AS VARCHAR(15))
,plan_component_desc
,NULL AS ext_id_type
,NULL AS svc_type
,'SMB' AS cfu
,NULL AS vip_code
,NULL AS owning_cost_ctr
,NULL AS owning_cost_ctr_dv
,NULL AS rental_tariff_group
, NULL AS work_ord_create_id
,NULL AS rep_cd
,w.dealer_cd
,post_cd
,work_ord_no
,NULL AS ext_ord_ref_no
,NULL AS svc_inst_id
,NULL AS subscr_no
,active_dt AS bolton_active_dt
,NULL AS bolton_inactive_dt
,work_ord_comp_dt
, work_ord_comp_tm
, work_ord_create_dt
,NULL AS work_ord_create_tm
,work_ord_type
,bill_product_action
,NULL AS svc_action_type
, NULL AS sales_chanl_id
, NULL AS wrkjn_rep_id
, NULL AS retl_store
,NULL AS disconnect_reason
,NULL AS disconnect_cd
,NULL AS original_drl_cd
,NULL AS original_rep_id
,NULL AS no_of_svc_x_acct
,CURRENT_DATE AS load_date
,'oo_awkord' src_tbl
FROM 
tbc_wo w
LEFT OUTER JOIN 
spcomm.f_tbc_acct_plans a 
ON w.acct_no = a.acct_no
AND w.active_dt BETWEEN a.plan_active_dt AND a.plan_inactive_dt
AND a.plan_type = 'Voice Plan'
QUALIFY RANK() OVER (
PARTITION BY w.acct_no, w.component_id , w.work_ord_no
                            ORDER BY  a.plan_inactive_dt DESC) = 1     ; 


.IF ERRORLEVEL > 0 THEN GOTO ERRIF
--- churns -- 


CREATE VOLATILE TABLE tbc_canx AS (
SELECT 
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
INNER JOIN  ipcfg.cfg_rate_plan_ref ref
ON com.component_id =  ref.rate_plan_cd
WHERE  wkr.work_ord_status IN ('4')
AND com.bill_product_action IN ('3','4')
AND rate_plan_lvl_3 = 'Pacman'
AND rate_plan_lvl_2 = 'TBC bolton'
AND work_ord_completion_dt BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param)
AND com.inactive_dt IS NOT NULL
) WITH DATA PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

INSERT INTO spcomm.f_bolton_add_churn
SELECT 
'Churns' AS status_cd
,NULL AS svc_no
,w.acct_no
,a.bill_to_nm 
,CAST(w.component_id AS VARCHAR(15)) AS bolton_rate_plan_cd
,w.component_id_dv AS bolton_rate_plan_ds
,CAST(plan_component_id AS VARCHAR(15))
,plan_component_desc
,NULL AS ext_id_type
,NULL AS svc_type
,'SMB' AS cfu
,NULL AS vip_code
,NULL AS owning_cost_ctr
,NULL AS owning_cost_ctr_dv
,NULL AS rental_tariff_group
, NULL AS work_ord_create_id
,NULL AS rep_cd
,w.dealer_cd
,post_cd
,work_ord_no
,NULL AS ext_ord_ref_no
,NULL AS svc_inst_id
,NULL AS subscr_no
,active_dt AS bolton_active_dt
,NULL AS bolton_inactive_dt
,work_ord_comp_dt
, work_ord_comp_tm
, work_ord_create_dt
,NULL AS work_ord_create_tm
,work_ord_type
,bill_product_action
,NULL AS svc_action_type
, NULL AS sales_chanl_id
, NULL AS wrkjn_rep_id
, NULL AS retl_store
,NULL AS disconnect_reason
,NULL AS disconnect_cd
,NULL AS original_drl_cd
,NULL AS original_rep_id
,NULL AS no_of_svc_x_acct
,CURRENT_DATE AS load_date
,'oo_awkord' src_tbl
FROM 
tbc_canx w
LEFT OUTER JOIN 
spcomm.f_tbc_acct_plans a 
ON w.acct_no = a.acct_no
AND w.active_dt BETWEEN a.plan_active_dt AND a.plan_inactive_dt
AND a.plan_type = 'Voice Plan'
QUALIFY RANK() OVER (
PARTITION BY w.acct_no, w.component_id , w.work_ord_no
                            ORDER BY  a.plan_inactive_dt DESC) = 1     ; 
                            
.IF ERRORLEVEL > 0 THEN GOTO ERRIF                            
                            
--- fetch market segment --- 
-- mobile --- 
CREATE VOLATILE TABLE acct AS (
SELECT acct_no  FROM spcomm.f_bolton_add_churn WHERE no_of_svc_x_acct IS NULL 
AND  work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)         
AND src_tbl IN (
'Opom Extract',
'adac_fbb',
'oo_awkord',
'OTT Trans')
) WITH DATA PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

CREATE VOLATILE TABLE mkt_seg
AS
(
SEL acct_id, COUNT(DISTINCT svc_inst_id) AS no_of_svc_x_acct
FROM
(SELECT acct_id, inst.svc_inst_id
FROM ipviews_srd.oo_svcins inst
INNER JOIN ipviews_srd.oo_svciex s
ON s.svc_inst_id = inst.svc_inst_id
WHERE inst.acct_id IN (SEL * FROM acct  GROUP BY 1)
AND s.close_dt = '2899-12-31' AND inst.close_dt = '2899-12-31' AND s.ext_id_type = '20'
AND (s.end_dt IS NULL OR s.end_dt <= (SELECT end_dt FROM dt_param))
AND (inst.end_dt IS NULL OR inst.end_dt <= (SELECT end_dt FROM dt_param))
QUALIFY RANK() OVER (
PARTITION BY acct_id, inst.svc_inst_id
                            ORDER BY  COALESCE(inst.end_dt, DATE '2899-12-31') DESC, COALESCE(s.end_dt, DATE '2899-12-31') DESC) = 1    
) x
GROUP BY 1
) WITH DATA PRIMARY INDEX (acct_id)
ON COMMIT PRESERVE ROWS
;         

.IF ERRORLEVEL > 0 THEN GOTO ERRIF

UPDATE
a
FROM
spcomm.f_bolton_add_churn a,
mkt_seg b
SET no_of_svc_x_acct = b.no_of_svc_x_acct
WHERE
a.work_ord_completion_dt BETWEEN (SEL start_dt FROM dt_param) AND (SEL end_dt FROM dt_param)     
AND a.acct_no = b.acct_id
AND a.ext_id_type = '20'
AND a.no_of_svc_x_acct IS NULL;

.IF ERRORLEVEL > 0 THEN GOTO ERRIF



COLLECT STATS ON spcomm.f_bolton_add_churn COLUMN (svc_no);
COLLECT STATS ON spcomm.f_bolton_add_churn COLUMN (bolton_rate_plan_cd);
COLLECT STATS ON spcomm.f_bolton_add_churn COLUMN (basic_rate_plan_cd);
COLLECT STATS ON spcomm.f_bolton_add_churn COLUMN (work_ord_completion_dt);


UPDATE  IPSHARE_PD.GF_TEMP_LOG
SET END_DATE = CURRENT_DATE,
END_TIME = CURRENT_TIME,
STATUS = 'OK'
WHERE IPSHARE_PD.GF_TEMP_LOG.INSERT_NUM = LOG_MAX.INSERT_NUM;

.LOGOFF
.QUIT


.LABEL ERRIF

UPDATE IPSHARE_PD.GF_TEMP_LOG
SET END_DATE = CURRENT_DATE,
END_TIME = CURRENT_TIME,
STATUS = 'FAILED'
WHERE IPSHARE_PD.GF_TEMP_LOG.INSERT_NUM = LOG_MAX.INSERT_NUM;

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

.LOGOFF
.QUIT





 

