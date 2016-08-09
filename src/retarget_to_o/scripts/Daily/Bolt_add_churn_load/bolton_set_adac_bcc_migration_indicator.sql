.RUN FILE = E:\SMB\report_scripts\template\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

UPDATE SOCOMM.F_BOLTON_ADD_CHURN
FROM
(
	SELECT
		idw.svc_no
		,idw.work_ord_completion_dt 
	FROM SOCOMM.F_BOLTON_ADD_CHURN idw
	INNER JOIN SOCOMM.F_BOLTON_ADD_CHURN edw
		ON idw.svc_no = edw.svc_no
		AND idw.work_ord_completion_dt = edw.work_ord_completion_dt
		AND idw.src_cd = 'ID'
		AND edw.src_cd = 'ED'
	WHERE idw.bcc_migrated_ind = 'N'
	GROUP BY 1,2
) dt
SET bcc_migrated_ind = 'Y'
WHERE SOCOMM.F_BOLTON_ADD_CHURN.svc_no = dt.svc_no
	AND SOCOMM.F_BOLTON_ADD_CHURN.work_ord_completion_dt = dt.work_ord_completion_dt
	AND SOCOMM.F_BOLTON_ADD_CHURN.src_cd = 'ID';
	

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

.QUIT ERRORCODE
.LOGOFF;

