

/* -----------------------------------------------------------------------------------------------------------------
-- 
-- COMMISSIONS Reporting - FY 2013/2014
-- 16-04-2013 : Shirley Pau - Initial Creation
--- Insert billed revenue into commissions for business partners and direct
-- 17-07-2013: Hussam Laham	- Scheduled the job for this script
-----------------------------------------------------------------------------------------------------------------*/

.RUN FILE = E:\SMB\report_scripts\template\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

--  NORAD STATEMENTS

CREATE VOLATILE TABLE LOG_MAX AS
(
SEL
CAST(TRIM(REPORT_ID)||TRIM(NEW_ID) AS INTEGER) AS INSERT_NUM,
REPORT_ID
FROM
(SEL 
COUNT(*)  AS NEW_ID,
10240 AS REPORT_ID
FROM IPSHARE_PD.GF_TEMP_LOG
WHERE REPORT_ID = 10240) A
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



CREATE VOLATILE TABLE  dt_param
AS
(
 SELECT ADD_MONTHS((CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE)+1),0) -1  AS dt_lkp
 )
WITH  DATA
ON COMMIT PRESERVE ROWS;


.IF ERRORLEVEL > 0 THEN .GOTO ERRIF


---- fetch billed revenue ---

CREATE VOLATILE MULTISET TABLE rev AS (
SELECT 
		bill_yr_no,
		bill_mnth_no,
		svc_no,
		SUM(net_amt) AS net_amt
		FROM  spsmbbi.f_mob_rev A
		LEFT OUTER JOIN spsmbbi.cfg_rev_ref B
		ON COALESCE(a.sap_gl, 'XXX') = COALESCE(b.sap_gl, 'XXX')
		AND COALESCE(a.sap_prod_cd, 'XXX') = COALESCE(b.sap_prod_cd,'XX')
		AND a.bill_stmt_chrg_cd =b.bill_stmt_chrg_cd
		WHERE chrg_type_lvl_3 <> 'Equipment'
		AND bill_yr_no = EXTRACT(YEAR FROM (SELECT * FROM dt_param))
		AND bill_mnth_no = EXTRACT(MONTH FROM (SELECT * FROM dt_param))
		GROUP BY 1,2,3
) WITH DATA PRIMARY INDEX (svc_no)
ON COMMIT PRESERVE ROWS;		


.IF ERRORLEVEL > 0 THEN .GOTO ERRIF 
 
 -- delete before insert

DELETE FROM  spcomm.f_commissions_fy1314
WHERE record_type = 'Bill Revenue'
AND day_dt = (SELECT * FROM dt_param);


.IF ERRORLEVEL > 0 THEN .GOTO ERRIF

INSERT INTO spcomm.f_commissions_fy1314
 SEL
'Bill Revenue' AS  record_type    
,per_end_dt AS day_dt                      
,mm.dlr_cd                        
,mm.acct_no                       
,mm.service_no 
, mm.accs_meth_id                
,mm.rate_plan_cd       
,CAST (NULL AS VARCHAR(15)) AS bolton_rate_plan_cd              
,mrpr.wght_avg_accs_fee AS basic_waaf         
,mrpr.prod_grp_lvl_3 AS basic_product
,mrpr.rate_plan_ds AS basic_plan_ds
,CAST (NULL AS INTEGER) AS bolton_waaf
,CAST (NULL AS VARCHAR(25)) AS bolton_product
,CAST (NULL AS VARCHAR(55)) AS bolton_plan_ds            
,NULL AS svc_stat_cd
,mm.bill_to_nm    
,mm.cfu                           
,NULL AS contract_term                         
,NULL AS ontrct_end_dt
,NULL AS contrct_eff_dt                
,CAST(NULL AS VARCHAR(15)) AS work_ord_no
,regn
,report_grp_cd
,0 AS service_count
,0 AS bolton_svc_count
--,0 AS bolton_offer_count
,0 AS dup_bolton_svc_count
,0 AS churn_count
,0 AS recontract_count
,0 AS mob_sov
--,0 AS mob_sov_count
,0 AS mob_connections
,0 AS da_connections
,0 AS digital_connections
,0 AS fixed_sov
,0 AS fixed_lines
,0
,net_amt  AS billed_rev
,CAST('mob_rev' AS VARCHAR(50)) AS src_tbl
FROM   spsmbbi.f_mob_mobilebase    mm        
INNER JOIN  spcomm.zz_cfg_rate_plan_ref mrpr
        ON  mm.rate_plan_cd = mrpr.rate_plan_cd            
INNER JOIN ipviews_md.md_dlr_hier_ref mdhr
            ON  mm.dlr_cd =  mdhr.dlr_cd
                AND mdhr.close_dt = DATE '2899-12-31'
                AND day_dt BETWEEN mdhr.dlr_hier_ref_eff_dt AND mdhr.dlr_hier_ref_end_dt 	 
INNER JOIN rev
ON rev.svc_no = mm.service_no
WHERE report_grp_cd = 'Business Channels'
AND EXTRACT(YEAR FROM per_end_dt) = bill_yr_no
AND EXTRACT(MONTH FROM per_end_dt) = bill_mnth_no
AND regn IN ('Business Partners' ,'Direct')
;



.IF ERRORLEVEL > 0 THEN .GOTO ERRIF


COLLECT STATISTICS ON spcomm.f_commissions_fy1314 COLUMN (svc_no);


.IF ERRORLEVEL > 0 THEN .GOTO ERRIF

--  NORAD STATEMENTS

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


.LOGOFF
.QUIT 2013;
