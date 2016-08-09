ECHO OFF
::--------------------------------------------------------------------
:: Get DATE
::--------------------------------------------------------------------
	FOR /f "tokens=2-4 delims=/:, " %%a IN ('date /t') DO (
                      SET DAY=%%a
                      SET MONTH=%%b
                      SET YEAR=%%c)

	SET DATE=%YEAR%%MONTH%%DAY%
		
:: ------------------------------------------------------------------
:: Get_Time
:: ------------------------------------------------------------------
	FOR /f "Tokens=1" %%i IN ('time /t') DO SET tm=%%i
	SET TIME=%tm::=%

ECHO ON

rem  this is comment  2012-02-27

BTEQ < "\\scfs8493\smb$\Commercial\01_Systems_Analytics\02_Operations\Scripts\Report_scripts\RP00001.sql" > "E:\SMB\log\RP00001_%DATE%%TIME%.log" 2> "E:\SMB\log\RP00001_%DATE%%TIME%.err"


