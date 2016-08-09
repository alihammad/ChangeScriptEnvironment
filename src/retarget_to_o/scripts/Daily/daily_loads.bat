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

SET CUBENMAE=Bolton_TBC_LOAD

call  D:\SIT\report_scripts\admin\ss_batch_log_start.bat Bolton_TBC_LOAD

BTEQ < "D:\SIT\report_scripts\Daily\Bolt_add_churn_load\bolton_add_churn_daily_load.sql" > "D:\SIT\log\bolton_add_churn_daily_load_%DATE%%TIME%.log"  2>  "D:\SIT\log\bolton_add_churn_daily_load_%DATE%%TIME%.err"

BTEQ < "D:\SIT\report_scripts\Daily\Bolt_add_churn_load\bolton_add_churn_daily_load_edw.sql" > "D:\SIT\log\bolton_add_churn_daily_load_edw_%DATE%%TIME%.log"  2>  "D:\SIT\log\bolton_add_churn_daily_load_edw_%DATE%%TIME%.err"

BTEQ < "D:\SIT\report_scripts\Daily\Bolt_add_churn_load\bolton_adac_bcc_migration_indicator.sql" > "D:\SIT\log\bolton_adac_bcc_migration_indicator_%DATE%%TIME%.log"  2>  "D:\SIT\log\bolton_adac_bcc_migration_indicator_%DATE%%TIME%.err"

if not %errorlevel%==0 goto :Label99


BTEQ < "D:\SIT\report_scripts\daily\TBC\TBC.sql" > "D:\SIT\log\TBC_%DATE%%TIME%.log"  2>  "D:\SIT\log\TBC_%DATE%%TIME%.err"


if not %errorlevel%==0 goto :Label99

BTEQ < "D:\SIT\report_scripts\daily\TBC\TBC_event.sql" > "D:\SIT\log\TBC_event_%DATE%%TIME%.log"  2>  "D:\SIT\log\TBC_event_%DATE%%TIME%.err"


if not %errorlevel%==0 goto :Label99


call  D:\SIT\report_scripts\admin\ss_batch_log_end.bat Bolton_TBC_LOAD  

exit 88

:Label99  

call  D:\SIT\report_scripts\admin\ss_batch_log_end_failed.bat Bolton_TBC_LOAD 

exit  %errorlevel%