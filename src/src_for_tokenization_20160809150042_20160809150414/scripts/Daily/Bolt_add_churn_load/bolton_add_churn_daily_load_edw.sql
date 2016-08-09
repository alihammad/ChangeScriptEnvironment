.RUN FILE = D:\SIT\report_scripts\template\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

CREATE VOLATILE TABLE dt_param AS ( 
  SELECT
   CURRENT_DATE - 12 AS start_dt,
   CURRENT_DATE-2 AS end_dt     
)
WITH  DATA
ON COMMIT PRESERVE ROWS;

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

DELETE FROM 
  SOCOMM.F_BOLTON_ADD_CHURN_EDW 
WHERE 
  work_ord_completion_dt 
   BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

INSERT INTO SOCOMM.F_BOLTON_ADD_CHURN_EDW(
status_cd,
svc_no,
acct_no,
customer_name,
bolton_rate_plan_cd,
bolton_rate_plan_ds,
basic_rate_plan_cd,
basic_plan_ds,
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
work_ord_no,
ext_ord_ref_no,
svc_inst_id,
subscr_no,
bolton_active_dt,
bolton_inactive_dt,
work_ord_completion_dt,
work_ord_completion_tm,
work_ord_create_dt,
work_ord_create_dt_tm,
work_ord_type,
bill_product_action,
svc_action_type,
sales_chanl_id,
wrkjn_rep_id,
retl_store,
disconnect_reason,
disconnect_cd,
original_dlr_cd,
original_rep_id,
no_of_svc_x_acct,
load_date,
src_tbl,
src_cd
)
SELECT 
  evnt_type_ds AS status_cd
  ,PRIM_RSRC_VALU_TXT AS svc_no
  ,CAST(ACCT_ID AS BIGINT) AS acct_no
  ,ACCT_NM AS customer_name
  ,CAST(BLTN_BILL_OFFR_ID AS BIGINT) AS bolton_rate_plan_cd
  ,BLTN_BILL_OFFR_DS AS bolton_rate_plan_ds
  ,CAST(MAIN_BILL_OFFR_ID AS BIGINT) AS basic_rate_plan_cd
  ,MAIN_BILL_OFFR_DS AS basic_plan_ds
  ,NULL AS ext_id_type
  ,NULL AS svc_type
  ,CASE 
    WHEN TRIM(ACCT_TYPE_CD) ='D' THEN 
      'Consumer'
    WHEN TRIM(ACCT_TYPE_CD) ='S' THEN 
      'SMB' 
    ELSE 
      'Unknown'
   END AS CFU
  ,NULL AS vip_code
  ,ACCT_TYPE_CD  AS owning_cost_ctr
  ,ACCT_TYPE_DS AS owning_cost_ctr_dv
  ,APPL_CUST_TYPE_DS AS rental_tariff_group
  ,CURR_AGNT_LGIN_NM AS work_ord_create_id
  ,NULL AS rep_id
  ,CURR_DEAL_ID AS dealer_cd
  ,ACCT_ZIP_CD AS post_cd
  ,ORD_ID AS work_ord_no
  ,NULL AS ext_ord_ref_no
  ,CAST(subs_id AS BIGINT) AS svc_inst_id
  ,NULL AS subscr_no
  ,CAST(ASSG_PRCE_PLAN_STRT_TS AS DATE) AS bolton_active_dt
  ,CAST(ASSG_PRCE_PLAN_END_TS AS DATE) AS bolton_inactive_dt
  ,CAST(CURR_ORD_ACTN_DONE_TS AS DATE) AS work_ord_completion_dt 
  ,NULL AS work_ord_completion_tm
  ,CAST(CURR_ORD_ACTN_STRT_TS AS DATE) AS work_ord_create_dt 
  ,NULL AS work_ord_create_dt_tm
  ,CURR_ORD_ACTN_TYPE_ID AS work_ord_type 
  ,CURR_ORD_ACTN_TYPE_DS AS bill_product_action
  ,NULL AS svc_action_type
  ,CURR_SALE_SERV_CHNL_REFR_ID  AS sales_chanl_id
  ,NULL AS wrkjn_rep_id
  ,NULL AS retl_store
  ,CURR_ORD_ACTN_RSN_DS AS disconnect_reason
  ,CURR_ORD_ACTN_RSN_ID AS disconnect_cd
  ,PROV_DEAL_ID AS original_dlr_cd 
  ,PROV_AGNT_LGIN_NM AS original_rep_id
  ,ACCT_MKT_SGMT_SERV_CNT AS no_of_svc_x_acct
  ,CAST(run_ts AS DATE) AS load_date
  ,NULL AS src_tbl
  ,'ED' AS src_cd
FROM 
  IOSHARE_EDW.IPS_F_SUBS_BLTN_TRAN_DAY TD  
WHERE 
  TD.CURR_ORD_ACTN_DONE_TS BETWEEN 
    (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN_EDW COLUMN (svc_no);
COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN_EDW COLUMN (bolton_rate_plan_cd);
COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN_EDW COLUMN (basic_rate_plan_cd);
COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN_EDW COLUMN (work_ord_completion_dt);

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

DELETE FROM 
  SOCOMM.F_BOLTON_ADD_CHURN
WHERE 
  work_ord_completion_dt 
   BETWEEN (SELECT start_dt FROM dt_param) AND (SELECT end_dt FROM dt_param);

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

INSERT INTO SOCOMM.F_BOLTON_ADD_CHURN(
  status_cd,
  svc_no,
  acct_no,
  customer_name,
  bolton_rate_plan_cd,
  bolton_rate_plan_ds,
  basic_rate_plan_cd,
  basic_plan_ds,
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
  work_ord_no,
  ext_ord_ref_no,
  svc_inst_id,
  subscr_no,
  bolton_active_dt,
  bolton_inactive_dt,
  work_ord_completion_dt,
  work_ord_completion_tm,
  work_ord_create_dt,
  work_ord_create_dt_tm,
  work_ord_type,
  bill_product_action,
  svc_action_type,
  sales_chanl_id,
  wrkjn_rep_id,
  retl_store,
  disconnect_reason,
  disconnect_cd,
  original_dlr_cd,
  original_rep_id,
  no_of_svc_x_acct,
  load_date,
  src_tbl,
  src_cd)
SELECT 
  status_cd,
  svc_no,
  acct_no,
  customer_name,
  bolton_rate_plan_cd,
  bolton_rate_plan_ds,
  basic_rate_plan_cd,
  basic_plan_ds,
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
  work_ord_no,
  ext_ord_ref_no,
  svc_inst_id,
  subscr_no,
  bolton_active_dt,
  bolton_inactive_dt,
  work_ord_completion_dt,
  work_ord_completion_tm,
  work_ord_create_dt,
  work_ord_create_dt_tm,
  work_ord_type,
  bill_product_action,
  svc_action_type,
  sales_chanl_id,
  wrkjn_rep_id,
  retl_store,
  disconnect_reason,
  disconnect_cd,
  original_dlr_cd,
  original_rep_id,
  no_of_svc_x_acct,
  load_date,
  src_tbl,
  src_cd
FROM 
SOCOMM.F_BOLTON_ADD_CHURN_EDW;

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN COLUMN (svc_no);
COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN COLUMN (bolton_rate_plan_cd);
COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN COLUMN (basic_rate_plan_cd);
COLLECT STATS ON SOCOMM.F_BOLTON_ADD_CHURN COLUMN (work_ord_completion_dt);

.IF ERRORLEVEL > 0 THEN .EXIT ERRORCODE

.LOGOFF
.QUIT






 

