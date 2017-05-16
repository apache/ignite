@echo off

set ODBC=%1

if [%ODBC%] == [] (
	echo error: driver is not specified. Call format: install_x86 abs_path_to_driver.
	pause
	exit /b 1
)

if exist %ODBC% (
	for %%i IN (%ODBC%) DO IF EXIST %%~si\NUL (
		echo warning: The path you have specified seems to be a directory. Note that you have to specify path to driver file itself instead.
	)
	echo Installing driver: %ODBC%
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Driver /t REG_SZ /d %ODBC% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Setup /t REG_SZ /d %ODBC% /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Ignite" /t REG_SZ /d "Installed" /f    
) else (
	echo Driver can not be found: %ODBC%
	echo Call format: install_x86 abs_path_to_driver
	pause
	exit /b 1
)
