ECHO on
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

ECHO %DATE%%TIME%

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
	

	SET DATE=%YEAR%%MONTH%%DAY%
		
:: ------------------------------------------------------------------
:: Get_Time
:: ------------------------------------------------------------------
	FOR /f "Tokens=1" %%i IN ('time /t') DO SET tm=%%i
	SET TIME=%tm::=%

ECHO %DATE%%TIME%
