@echo off

set ODBC="%1"

if exist %ODBC% (
	:: Installing driver.
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Driver /t REG_SZ /d "%ODBC%" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Setup /t REG_SZ /d "%ODBC%" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Ignite" /t REG_SZ /d "Installed" /f    
) else (
	echo Driver can not be found: %ODBC%
	echo Call format: install_x86 path_to_driver
	pause
)
