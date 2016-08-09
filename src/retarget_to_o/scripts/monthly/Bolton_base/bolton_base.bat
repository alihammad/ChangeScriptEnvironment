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

call  D:\SIT\report_scripts\admin\ss_batch_log_start.bat  digital_base


BTEQ < "D:\SIT\report_scripts\monthly\bolton_base\bolton_base.sql" > "D:\SIT\log\bolton_base_%DATE%%TIME%.log"  2>  "D:\SIT\log\bolton_base_%DATE%%TIME%.err"

BTEQ < "D:\SIT\report_scripts\monthly\bolton_base\bolton_base_edw.sql" > "D:\SIT\log\bolton_base_edw_%DATE%%TIME%.log"  2>  "D:\SIT\log\bolton_base_edw_%DATE%%TIME%.err"

BTEQ < "D:\SIT\report_scripts\monthly\bolton_base\bolton_set_bcc_migration_indicator.sql" > "D:\SIT\log\bolton_set_bcc_migration_indicator_%DATE%%TIME%.log"  2>  "D:\SIT\log\bolton_set_bcc_migration_indicator_%DATE%%TIME%.err"

if not %errorlevel%==0 goto :Label99

call  D:\SIT\report_scripts\admin\ss_batch_log_end.bat  digital_base

exit 88

:Label99  

call  D:\SIT\report_scripts\admin\ss_batch_log_end_failed.bat  digital_base

exit  %errorlevel%
