@echo off

set ODBC_AMD64="%1"
set ODBC_X86="%2"

if exist %ODBC_AMD64% (
	:: Installing 64-bit driver.
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Driver /t REG_SZ /d "%ODBC_AMD64%" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\Apache Ignite" /v Setup /t REG_SZ /d "%ODBC_AMD64%" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Ignite" /t REG_SZ /d "Installed" /f    
) else (
	echo 64-bit driver can not be found: %ODBC_AMD64%
	echo Call format: install_amd64 path_to_64_bit_driver [path_to_32_bit_driver]
	pause
	exit /b 1
)

if exist %ODBC_X86% (
	rem Installing 32-bit driver.
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\Apache Ignite" /v DriverODBCVer /t REG_SZ /d "03.80" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\Apache Ignite" /v UsageCount /t REG_DWORD /d 00000001 /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\Apache Ignite" /v Driver /t REG_SZ /d "%ODBC_X86%" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\Apache Ignite" /v Setup /t REG_SZ /d "%ODBC_X86%" /f
	reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\ODBC Drivers" /v "Apache Ignite" /t REG_SZ /d "Installed" /f    
) else (
	echo 32-bit driver can not be found: %ODBC_X86%
)