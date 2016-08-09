::=====================================================================
:: Change Log
::--------------
::
:: 2013-05-24 - RL - Created batch file
:: 2013-06-03 - RL - Added error checking at job level
:: 2013-06-19 - RL - Added scripts as per new naming standards
:: 2013-07-02 - AZ - Adapted RL's batch file for FXD use
:: 2013-07-16 - HL - ADDED scripts related to Boltons SLD (Owner: Barrie L)
::======================================================================

@ECHO OFF

::--------------------------------------------------------------------
:: Set DATE vars
::--------------------------------------------------------------------
fOR /F "TOKENS=1-4* DELIMS=/- " %%A IN ('DATE/T') DO (
SET DAY=%%A
SET MONTH=%%B
SET YEAR=%%C
)

SET TODAY=%YEAR%%MONTH%%DAY%
		
:: ------------------------------------------------------------------
:: Set Time vars
:: ------------------------------------------------------------------
	FOR /f "Tokens=1" %%i IN ('time /t') DO SET tm=%%i
	SET NOW=%tm::=%

:: ------------------------------------------
:: Set path to base script directory
::-------------------------------------------
SET BASE_DIR="E:\SMB\report_scripts\"

:: ------------------------------------------
:: Set path to log directory
::-------------------------------------------
SET LOG_DIR="E:\SMB\log\"

:: ------------------------------------------
:: Set path to blat (email utility)
::-------------------------------------------
SET BLAT="E:\SMB\Apps\blat307\full\blat"

::--------------------------
:: set required variables
::--------------------------

SET BTEQ_FOLDER=E:\SMB\report_scripts\monthly\ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE\BTEQ\

ECHO ON

::-----------------------
:: Run BTEQ scripts
::-----------------------

BTEQ < %BTEQ_FOLDER%ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE.SQL > %LOG_DIR%ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE_%TODAY%_%NOW%.LOG  2>  %LOG_DIR%ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE_%TODAY%_%NOW%.ERR

if not %ERRORLEVEL%==0 goto ERR_RTN

%blat% - -body "SUCCESS: ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE" -s "SUCCESS: ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE - %TODAY%_%NOW%" -server mailgw ^
		-to BIDataAndSystems@optus.com.au,mary.chao@optus.com.au,Tom.Hackl@optus.com.au -f noreply@optus.com.au

exit 0

:ERR_RTN

%blat% - -body "ERROR: ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE" -s "ERROR: ETL_F_ALL_M_COMMISSIONS_FY1314_BILLED_REVENUE - %TODAY%_%NOW%" -server mailgw  ^
	-to BIDataAndSystems@optus.com.au,mary.chao@optus.com.au,Tom.Hackl@optus.com.au -f noreply@optus.com.au

exit %errorlevel%