ECHO OFF
::--------------------------------------------------------------------
:: Get DATE
::--------------------------------------------------------------------
fOR /F "TOKENS=1-4* DELIMS=/- " %%A IN ('DATE/T') DO (
SET DAY=%%B
SET MONTH=%%C
SET YEAR=%%D
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

echo  "Start !!!" >  "\\cbidevmodw001\D$\SMB\test_run\test_run.log"


call  \\cbidevmodw001\D$\SMB\test_run\test_refresh.bat

echo  "Refresh cube passed" >>  "\\cbidevmodw001\D$\SMB\test_run\test_run.log"

if not %errorlevel%==0 goto :Label99

rem call \\cbidevmodw001\D$\SMB\test_run\refresh_run_email.bat


exit 0

:Label99  

rem   call \\cbidevmodw001\D$\SMB\test_run\refresh_notrun_email.bat


exit  %errorlevel%