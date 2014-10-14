@echo OFF

echo Kill all grid nodes.
FOR /f "tokens=1,2" %%G IN ('%JAVA_HOME%/bin/jps') DO IF (%%H) EQU (GridCommandLineStartup) taskkill /PID %%G /T /F

echo Kill all grid nodes.
FOR /f "tokens=1,2" %%G IN ('%JAVA_HOME%/bin/jps') DO IF (%%H) EQU (GridRouterCommandLineStartup) taskkill /PID %%G /T /F

echo Wait 1 seconds while nodes stopped.
ping -n 1 127.0.0.1 > NUL

echo Kill all cmd processes.
taskkill /IM cmd.exe /T /F > NUL

echo ON
