.RUN FILE = E:\SMB\report_scripts\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

/*  ----------------------------------------------------------------------------------------------------
--
-- Bolton Reporting Base
--  
--  04-09-2013 : 
--  Change IOCFG.cfg_rate_plan_ref to SOCOMM.bolton_rp_ref as IOCFG is not maintain any more  
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

----------------------------------------------------------------------------------------------------*/


CREATE VOLATILE TABLE parameters AS 
(
 SELECT    
 ADD_MONTHS((CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE)+1),0) -1  AS per_end_dt        
) WITH DATA
PRIMARY INDEX (per_end_dt)
ON COMMIT PRESERVE ROWS;



.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE



CREATE VOLATILE TABLE bolton_plans AS 
(
SELECT     rate_plan_cd
                    ,rate_plan_ds
--FROM       IOCFG.cfg_rate_plan_ref 
--  04-09-2013
--  Change IOCFG.cfg_rate_plan_ref to SOCOMM.bolton_rp_ref as IOCFG is not maintain any more  
From SOCOMM.bolton_rp_ref
Where   bolton_in = '1'
--  WHERE    prod_grp_lvl_3 IN ('VAS','Digital Business')
AND rate_plan_cd <>  '700120'
) 
WITH DATA
PRIMARY INDEX (rate_plan_cd)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON bolton_plans COLUMN (rate_plan_cd);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- temprorary until the bolton groups are regrouped. these are the fixed NBN boltons
INSERT INTO bolton_plans 
SELECT rate_plan_cd, rate_plan_ds FROM SOCOMM.zz_cfg_rate_plan_ref
WHERE rate_plan_cd IN (
'601952',
'601869',
'601975',
'601870',
'601976',
'602331',
'602329',
'602330');

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--- fetch active boltons for the reporting period --


CREATE VOLATILE MULTISET TABLE subscr_base AS (
SELECT     
p200.subscr_no
,p200.account_no
,p200.product_start
,p200.product_stop
,p200.component_id
,cce_active_dt
,cce_inactive_dt
FROM         IOVIEWS_SRD.ar_p200 p200 
CROSS JOIN parameters p
WHERE      p.per_end_dt BETWEEN p200.product_start (FORMAT 'YYYY-MM-DD') (CHAR(10)) AND COALESCE(p200.product_stop, DATE '2899-12-31')
 AND  p.per_end_dt BETWEEN p200.open_dt-2 AND p200.close_dt
 AND CAST(component_id AS VARCHAR(6))IN (SELECT rate_plan_cd FROM  bolton_plans)
 QUALIFY RANK() OVER (
PARTITION BY subscr_no, component_id
                                          ORDER BY  cce_active_dt DESC, COALESCE(cce_inactive_dt, DATE '2899-12-31') DESC ) = 1 
 ) WITH DATA
PRIMARY INDEX (subscr_no)
INDEX (component_id)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON subscr_base COLUMN (subscr_no);
COLLECT STATISTICS ON subscr_base COLUMN (component_id);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- fetch the basic access rate plan ---

CREATE VOLATILE TABLE basic_plans AS (
SELECT 
com.component_id,
com.component_id_dv AS basic_plan_ds,
package_group
FROM IOVIEWS_SRD.AR_PACKAGE_DEFINITION_REF pk
INNER JOIN
IOVIEWS_SRD.AR_PACKAGE_COMPONENTS pk_com
ON pk.package_id = pk_com.package_id
AND pk_com.close_dt = '2899-12-31'
INNER JOIN
IOVIEWS_SRD.AR_COMPONENT_DEFINITION_VALUES com
ON pk_com.component_id = com.component_id
AND com.close_dt = '2899-12-31'
-- 11/07/2014 Add 24,25,26,27
WHERE  pk.package_group IN (3,4,5,6,22,24,25,26,27)

AND pk.close_dt = '2899-12-31'
) WITH DATA PRIMARY INDEX (component_id)
ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON basic_plans COLUMN (component_id);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--- sample having two rate plans - subscr_no = '56847023'

CREATE VOLATILE MULTISET TABLE basic_access AS (
SELECT     
p200.subscr_no
,p200.account_no
,p200.component_id
,p200.product_start
,p200.product_stop
,cce_active_dt
,cce_inactive_dt
,p200.open_dt
,p200.close_dt
FROM         IOVIEWS_SRD.ar_p200 p200 
CROSS JOIN parameters p
WHERE      p.per_end_dt BETWEEN p200.product_start (FORMAT 'YYYY-MM-DD') (CHAR(10)) AND COALESCE(p200.product_stop, DATE '2899-12-31')
 AND  p.per_end_dt BETWEEN p200.open_dt-2 AND p200.close_dt
 AND p200.subscr_no IN (SELECT subscr_no FROM subscr_base GROUP BY 1)
 AND p200.component_id IN (SELECT component_id FROM basic_plans)
  QUALIFY RANK() OVER (
PARTITION BY subscr_no
                                          ORDER BY  cce_active_dt DESC, COALESCE(cce_inactive_dt, DATE '2899-12-31') DESC, product_start DESC,  COALESCE(p200.product_stop, DATE '2899-12-31') DESC, p200.open_dt DESC, p200.close_dt DESC, component_id DESC) = 1 
GROUP BY 1,2 ,3,4,5,6,7,8,9
 ) WITH DATA
PRIMARY INDEX (subscr_no)
INDEX (component_id)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--- get product ---


CREATE VOLATILE MULTISET TABLE  basic_svc AS (
SELECT 
s100.emf_config_id
,CAST(NULL AS VARCHAR(25)) AS product
,s100.display_external_id_type
,s100.subscr_no
,s200.external_id
,s200.external_id_type
,s200.active_date
,s200.inactive_date
 FROM 
IOVIEWS_SRD.ar_s100 s100
INNER JOIN
IOVIEWS_SRD.ar_s200 s200
ON s100.subscr_no = s200.subscr_no
AND s100.display_external_id_type = s200.external_id_type
CROSS JOIN parameters p
WHERE p.per_end_dt BETWEEN s200.open_dt -2 AND s200.close_dt 
AND p.per_end_dt BETWEEN CAST(s200.active_date AS DATE) AND COALESCE(CAST(s200.inactive_date AS DATE), DATE '2899-12-31')
AND p.per_end_dt BETWEEN s100.open_dt -2 AND s100.close_dt
AND s100.subscr_no IN (SELECT subscr_no FROM subscr_base GROUP BY 1)
AND s100.record_type <> '40'
AND s200.record_type <> '40'
 QUALIFY RANK() OVER (
PARTITION BY s200.subscr_no
                                          ORDER BY  s200.active_date DESC, COALESCE(s200.inactive_date, DATE '2899-12-31') DESC, s200.open_dt DESC, s100.open_dt DESC ) = 1 
) WITH DATA PRIMARY INDEX (subscr_no)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


UPDATE a
FROM basic_svc a,
(SELECT emf_config_id AS id, display_value FROM IOVIEWS_SRD.AR_EMF_CONFIG_ID_VALUES 
WHERE close_dt = '2899-12-31') b
SET product = display_value
WHERE emf_config_id = id;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
--- fetch cfu --


CREATE VOLATILE MULTISET TABLE acct AS (
SELECT 
a100.account_no,
a100.bill_lname AS customer_name,
a100.bill_zip,
a100.vip_code,
a100.owning_cost_ctr,
a100.owning_cost_ctr_dv,
a200.external_id AS acct_no
 FROM 
IOVIEWS_SRD.ar_a100 a100
INNER JOIN 
IOVIEWS_SRD.ar_a200 a200
ON a100.account_no = a200.account_no
AND a200.external_id_type = '1'
AND a200.close_dt = '2899-12-31'
CROSS JOIN
parameters p
WHERE p.per_end_dt BETWEEN a100.open_dt -2 AND a100.close_dt
AND a100.account_no IN (SELECT account_no FROM subscr_base)
QUALIFY RANK() OVER (
PARTITION BY acct_no
                            ORDER BY  a100.close_dt DESC ) = 1 
) WITH DATA PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

CREATE VOLATILE TABLE output AS (
SELECT 
CAST(NULL AS DATE) AS per_end_dt,
bs.external_id AS svc_no,
bs.product,
bs.external_id_type,
CAST(sub.cce_active_dt AS DATE) AS svc_start_date,
sub.product_start,
CAST(sub.component_id AS VARCHAR(15)) AS bolton_rate_plan_cd,
CAST(NULL AS VARCHAR(50)) AS bolton_rate_plan_ds,
CAST(ba.component_id AS VARCHAR(15)) AS basic_rate_plan_cd,
CAST(NULL AS VARCHAR(50)) AS basic_plan_ds,
acct.acct_no,
acct.customer_name,
acct.bill_zip AS post_cd,
CAST (NULL AS INTEGER) AS no_of_x_svc,
acct.vip_code,
acct.owning_cost_ctr,
acct.owning_cost_ctr_dv,
CAST(NULL AS VARCHAR(55)) AS cfu,
CAST (NULL AS VARCHAR(25)) AS dlr_cd,
CAST (NULL AS VARCHAR(25)) AS rep_id,
CAST (NULL AS VARCHAR(25)) AS sales_chanl_id,
CAST (NULL AS VARCHAR(25)) AS retl_store,
CAST (NULL AS VARCHAR(25)) AS acq_partnr_cmpny,
CAST (NULL AS VARCHAR(25)) AS svc_inst_id,
CAST (NULL AS VARCHAR(25)) AS work_ord_no,
sub.subscr_no AS ar_subscr_no,
sub.account_no AS ar_account_no
FROM 
subscr_base sub
LEFT OUTER JOIN basic_access ba
ON sub.subscr_no = ba.subscr_no
LEFT OUTER JOIN
basic_svc bs
ON sub.subscr_no = bs.subscr_no
LEFT OUTER JOIN acct
ON sub.account_no = acct.account_no
QUALIFY ROW_NUMBER()  OVER (
PARTITION BY svc_no, bolton_rate_plan_cd, basic_rate_plan_cd  -- reason for qualifying is for cases where service changes account no hence two subscr no e.g. 0412100048 with 93882969 and 54136768
                            ORDER BY  svc_start_date DESC, sub.product_start DESC ) = 1 
) WITH DATA PRIMARY INDEX (svc_no, bolton_rate_plan_cd, basic_rate_plan_cd)
ON COMMIT PRESERVE ROWS;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--  populate plan description
UPDATE a
FROM output a,
bolton_plans b
SET bolton_rate_plan_ds = b.rate_plan_ds
WHERE a.bolton_rate_plan_cd = b.rate_plan_cd;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

UPDATE a
FROM output a,
basic_plans b
SET basic_plan_ds = b.basic_plan_ds
WHERE a.basic_rate_plan_cd = b.component_id;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


-- delete from output as the services are no longer active at the reporting month end
DEL FROM output
WHERE svc_no IS NULL;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


-- set reporting date

UPDATE a
FROM output a,
parameters b
SET per_end_dt = b.per_end_dt;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


COLLECT STATS ON output COLUMN (svc_no);
COLLECT STATS ON output COLUMN (bolton_rate_plan_cd);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--- find dealer code and rep id


CREATE VOLATILE TABLE oo_ord AS (
SELECT
output.svc_no,
ord.work_ord_no,
ord.dealer_cd,
ord.svc_inst_id,
cmp.active_dt,
cmp.inactive_dt,
work_ord_completion_dt,
cmp.component_id
FROM
 IOVIEWS_SRD.oo_wrkord ord
	INNER JOIN IOVIEWS_SRD.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
	INNER JOIN IOVIEWS_SRD.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
     INNER JOIN  IOVIEWS_SRD.oo_svciex s
     ON s.svc_inst_id = ord.svc_inst_id
     INNER JOIN output
     ON s.ext_id = output.svc_no
WHERE     ord.work_ord_status IN ('2','3','4','9') -- completed
AND cmp.bill_product_action IN ('1','2') -- 1 provide, 2 - change
AND ord.work_ord_type IN ('1','2') -- 1 provide, 2 - change
AND ord.work_ord_create_dt <= output.per_end_dt +5 
AND (output.per_end_dt BETWEEN cmp.active_dt-15 AND COALESCE(cmp.inactive_dt, DATE '2899-12-31') OR cmp.active_dt IS NULL)
AND cmp.component_id = output.bolton_rate_plan_cd
AND COALESCE(s.end_dt, DATE '2899-12-31') >= output.per_end_dt
--and output.svc_no = '0411403086'
 QUALIFY ROW_NUMBER()  OVER (
PARTITION BY svc_no, bolton_rate_plan_cd
                            ORDER BY  COALESCE(ord.work_ord_create_dt, DATE '2899-12-31')  DESC, ord.work_ord_create_tm DESC, COALESCE(cmp.active_dt, DATE '2899-12-31') ASC, work_ord_status DESC, COALESCE(work_ord_completion_dt, DATE '1901-01-01') DESC, work_ord_completion_tm DESC, ord.work_ord_no DESC ) = 1   
) WITH DATA PRIMARY INDEX (svc_no)
ON COMMIT PRESERVE ROWS;

/*  select * from IOVIEWS_SRD.oo_wrkord ord
	INNER JOIN IOVIEWS_SRD.oo_btxnpa pkg
          ON  pkg.work_ord_no = ord.work_ord_no
	INNER JOIN IOVIEWS_SRD.oo_btxncm cmp
          ON  cmp.int_pack_inst = pkg.int_pack_inst 
where ord.work_ord_no in (
'IPB6388001',
'IPB6389001')
  */

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

UPDATE a
FROM output a,
oo_ord b
SET work_ord_no = b.work_ord_no,
dlr_cd = dealer_cd,
svc_inst_id = b.svc_inst_id
WHERE a.svc_no = b.svc_no
AND a.bolton_rate_plan_cd = CAST(b.component_id AS VARCHAR(15));

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

--- get rep id


CREATE VOLATILE TABLE rep AS (
SELECT
work_ord_no,
reps_id,
sales_chanl_id,
retl_store,
acq_partnr_cmpny
FROM
 IOVIEWS_SRD.OO_WRKOJN   -- select * from IOVIEWS_SRD.oo_wrkojn where work_ord_no = 'PDBB292001'
 WHERE work_ord_no IN (SELECT work_ord_no FROM oo_ord GROUP BY 1)
  QUALIFY ROW_NUMBER() OVER (
PARTITION BY work_ord_no
                            ORDER BY  open_dt ASC) = 1   
 ) WITH DATA PRIMARY INDEX (work_ord_no)
 ON COMMIT PRESERVE ROWS;
 
 
 .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
 
 UPDATE a
FROM output a,
rep b
SET rep_id = b.reps_id,
sales_chanl_id = b.sales_chanl_id,
retl_store = b.retl_store,
acq_partnr_cmpny = b.acq_partnr_cmpny
WHERE a.work_ord_no = b.work_ord_no;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
 
--- get no of svc x acct


CREATE VOLATILE MULTISET TABLE svc_x AS (
SELECT 
subscr_no, 
account_no,
display_external_id_type,
service_start,
service_end
FROM IOVIEWS_SRD.ar_s100
WHERE 
record_type <> '40'
AND (SELECT per_end_dt FROM parameters) BETWEEN CAST(service_start AS DATE)  AND COALESCE(service_end, DATE '2899-12-31')
AND (SELECT per_end_dt FROM parameters) BETWEEN open_dt -2  AND close_dt
AND account_no IN (SELECT ar_account_no FROM output GROUP BY 1)
 QUALIFY ROW_NUMBER()  OVER (
PARTITION BY subscr_no
                            ORDER BY  open_dt DESC, close_dt DESC ) = 1  
 ) WITH DATA PRIMARY INDEX (account_no)
 ON COMMIT PRESERVE ROWS; 
 
 .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
 
 -- mobile services
 
 UPDATE a
 FROM output a,
 ( SELECT account_no, COUNT(*) AS svc_cnt
 FROM svc_x
 WHERE display_external_id_type = '20'
 GROUP BY 1) b
 SET no_of_x_svc = svc_cnt
 WHERE a.ar_account_no = b.account_no
 AND a.external_id_type = '20';
 
 .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
 
 -- fix services
 
 UPDATE a
 FROM output a,
 ( SELECT account_no, COUNT(*) AS svc_cnt
 FROM svc_x
 WHERE display_external_id_type <> '20'
 GROUP BY 1) b
 SET no_of_x_svc = svc_cnt
 WHERE a.ar_account_no = b.account_no
 AND a.external_id_type <> '20';
 
 .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- update CFU

UPDATE a
FROM output a
, IOCFG.cfg_sales_class_ref r
SET cfu = r.cfu
WHERE CAST(vip_code AS VARCHAR(15))= sales_class_ref_cd;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE
     
-- delete before insert into target

DELETE FROM SOCOMM.f_bolton_base 
WHERE per_end_dt = (SELECT * FROM parameters);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


INSERT INTO SOCOMM.f_bolton_base
SELECT 
*
FROM 
output
;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE


COLLECT STATS ON SOCOMM.f_bolton_base COLUMN (svc_no);
COLLECT STATS ON SOCOMM.f_bolton_base COLUMN (bolton_rate_plan_cd);
COLLECT STATS ON SOCOMM.f_bolton_base COLUMN (per_end_dt);
COLLECT STATS ON SOCOMM.f_bolton_base COLUMN (basic_rate_plan_cd);

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

-- TBC Mobile base -- 

DELETE FROM SOCOMM.f_tbc_base WHERE per_end_dt = ( ADD_MONTHS((CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE)+1),0) -1) ;

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

INSERT INTO  SOCOMM.f_tbc_base 
SELECT
 ADD_MONTHS((CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE)+1),0) -1  AS per_end_dt,
 a.* FROM SOCOMM.f_tbc_acct_plans a
WHERE plan_type IN ('Voice Plan','Data Plan')
AND  ADD_MONTHS((CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE)+1),0) -1  BETWEEN plan_active_dt AND plan_inactive_dt
AND plan_active_dt  (FORMAT 'mmm-yy') (CHAR(7))  <> plan_inactive_dt (FORMAT 'mmm-yy') (CHAR(7))
QUALIFY ROW_NUMBER()  OVER (
PARTITION BY acct_no, plan_type
                            ORDER BY  plan_active_dt DESC) = 1  ;


.quit errorcode
.logoff;

