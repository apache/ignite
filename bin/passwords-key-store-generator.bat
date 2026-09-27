::
:: Copyright 2019 GridGain Systems, Inc. and Contributors.
:: 
:: Licensed under the GridGain Community Edition License (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
:: 
::     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
:: 
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::
@echo off
setlocal

:: Check JAVA_HOME.
if defined JAVA_HOME  goto checkJdk
    echo %0, ERROR:
    echo JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
goto end

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\keytool.exe" goto choice
    echo %0, ERROR:
    echo JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
goto end

:choice
SET /P ENCRYPT_PASSWORDS= Do you want to encrypt your passwords [Y/n]:
if NOT DEFINED ENCRYPT_PASSWORDS SET "ENCRYPT_PASSWORDS=Y"
SET ENCRYPT_PASSWORDS=%ENCRYPT_PASSWORDS:Y=y%
if /I "%ENCRYPT_PASSWORDS%" == "y" goto masterPassword
if /I "%ENCRYPT_PASSWORDS%" NEQ "y" goto end
goto :choice

:masterPassword
SET /P MASTER_PASSWORD= Please enter a master password for keystore: 
if [%MASTER_PASSWORD%] == [] goto end
if [%MASTER_PASSWORD%] NEQ [] goto nodePassword
goto :nodePassword

:nodePassword
SET /P NODE_PASSWORD= Do you want to add a node-password to keystore? [Y/n]: 
if NOT DEFINED NODE_PASSWORD SET "NODE_PASSWORD=Y"
SET NODE_PASSWORD=%NODE_PASSWORD:Y=y%
if /I "%NODE_PASSWORD%" == "y" (
    "%JAVA_HOME%\bin\keytool.exe" -importpassword -alias node-password -keystore passwords.p12 -storetype pkcs12 -storepass %MASTER_PASSWORD% -keypass %MASTER_PASSWORD%
)
goto nodeKeyStorePassword

:nodeKeyStorePassword
SET /P NODE_KEY_STORE_PASSWORD= Do you want to add a node-key-store-password to keystore? [Y/n]: 
if NOT DEFINED NODE_KEY_STORE_PASSWORD SET "NODE_KEY_STORE_PASSWORD=Y"
SET NODE_KEY_STORE_PASSWORD=%NODE_KEY_STORE_PASSWORD:Y=y%
if /I "%NODE_KEY_STORE_PASSWORD%" == "y" (
    "%JAVA_HOME%\bin\keytool.exe" -importpassword -alias node-key-store-password -keystore passwords.p12 -storetype pkcs12 -storepass %MASTER_PASSWORD% -keypass %MASTER_PASSWORD%
)
goto nodeTrustStorePassword

:nodeTrustStorePassword
SET /P NODE_TRUST_STORE_PASSWORD= Do you want to add a node-trust-store-password to keystore? [Y/n]: 
if NOT DEFINED NODE_TRUST_STORE_PASSWORD SET "NODE_TRUST_STORE_PASSWORD=Y"
SET NODE_TRUST_STORE_PASSWORD=%NODE_TRUST_STORE_PASSWORD:Y=y%
if /I "%NODE_TRUST_STORE_PASSWORD%" == "y" (
    "%JAVA_HOME%\bin\keytool.exe" -importpassword -alias node-trust-store-password -keystore passwords.p12 -storetype pkcs12 -storepass %MASTER_PASSWORD% -keypass %MASTER_PASSWORD%
)
goto serverKeyStorePassword

:serverKeyStorePassword
SET /P SERVER_KEY_STORE_PASSWORD= Do you want to add a server-key-store-password to keystore? [Y/n]: 
if NOT DEFINED SERVER_KEY_STORE_PASSWORD SET "SERVER_KEY_STORE_PASSWORD=Y"
SET SERVER_KEY_STORE_PASSWORD=%SERVER_KEY_STORE_PASSWORD:Y=y%
if /I "%SERVER_KEY_STORE_PASSWORD%" == "y" (
    "%JAVA_HOME%\bin\keytool.exe" -importpassword -alias server-key-store-password -keystore passwords.p12 -storetype pkcs12 -storepass %MASTER_PASSWORD% -keypass %MASTER_PASSWORD%
)
goto serverTrustStorePassword

:serverTrustStorePassword
SET /P SERVER_TRUST_STORE_PASSWORD= Do you want to add a server-trust-store-password to keystore? [Y/n]: 
if NOT DEFINED SERVER_TRUST_STORE_PASSWORD SET "SERVER_TRUST_STORE_PASSWORD=Y"
SET SERVER_TRUST_STORE_PASSWORD=%SERVER_TRUST_STORE_PASSWORD:Y=y%
if /I "%SERVER_TRUST_STORE_PASSWORD%" == "y" (
    "%JAVA_HOME%\bin\keytool.exe" -importpassword -alias server-trust-store-password -keystore passwords.p12 -storetype pkcs12 -storepass %MASTER_PASSWORD% -keypass %MASTER_PASSWORD%
)
goto end


:end
endlocal