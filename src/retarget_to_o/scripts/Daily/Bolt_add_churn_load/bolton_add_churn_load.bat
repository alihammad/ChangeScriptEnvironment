ECHO OFF
::--------------------------------------------------------------------
:: Get DATE
::--------------------------------------------------------------------
fOR /F "TOKENS=1-4* DELIMS=/- " %%A IN ('DATE/T') DO (
SET DAY=%%A
SET MONTH=%%B
SET YEAR=%%C
)

echo %YEAR%
echo %MONTH%
echo %DAY%


rem	FOR /f "tokens=2-4 delims=/:, " %%a IN ('date /t') DO (
rem                      SET DAY=%%a
rem                      SET MONTH=%%b
rem                      SET YEAR=%%c)

	SET DATE=%YEAR%%MONTH%%DAY%
		
:: ------------------------------------------------------------------
:: Get_Time
:: ------------------------------------------------------------------
	FOR /f "Tokens=1" %%i IN ('time /t') DO SET tm=%%i
	SET TIME=%tm::=%

ECHO ON

SET CUBENMAE=Bolton_add_churn_LOAD

call E:\SMB\report_scripts\admin\ss_batch_log_start.bat Bolton_add_churn_LOAD 

BTEQ < "E:\SMB\report_scripts\Daily\Bolt_add_churn_load\bolton_add_churn_daily_load.sql" > "E:\SMB\log\bolton_add_churn_daily_load.sql_%DATE%%TIME%.log"  2>  "E:\SMB\log\bolton_add_churn_daily_load.sql_%DATE%%TIME%.err"


if not %errorlevel%==0 goto :Label99

call E:\SMB\report_scripts\admin\ss_batch_log_end.bat Bolton_add_churn_LOAD

exit 88

:Label99 
 
call E:\SMB\report_scripts\admin\ss_batch_log_end_failed.bat Bolton_add_churn_LOAD

exit  %errorlevel%