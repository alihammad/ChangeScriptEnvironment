.RUN FILE = E:\SMB\report_scripts\template\fexp_logon_batch.txt;
.SET ERROROUT STDOUT;

UPDATE SOCOMM.F_BOLTON_BASE
FROM
(
	SELECT
		idw.svc_no
		,idw.per_end_dt
	FROM SOCOMM.F_BOLTON_BASE idw
	INNER JOIN SOCOMM.F_BOLTON_BASE edw
		ON idw.svc_no = edw.svc_no
		AND idw.per_end_dt = edw.per_end_dt
		AND idw.src_cd = 'ID'
		AND edw.src_cd = 'ED'
	WHERE idw.bcc_migrated_ind = 'N'
	GROUP BY 1,2
) dt
SET bcc_migrated_ind = 'Y'
WHERE SOCOMM.F_BOLTON_BASE.svc_no = dt.svc_no
	AND SOCOMM.F_BOLTON_BASE.per_end_dt = dt.per_end_dt
	AND SOCOMM.F_BOLTON_BASE.src_cd = 'ID';

.IF ERRORCODE <> 0 THEN .EXIT ERRORCODE

.QUIT ERRORCODE
.LOGOFF;

