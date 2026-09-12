:: 激活集群
:: control.bat --activate

:: run sql tools
sqlline.bat --verbose=true -u jdbc:ignite:thin://127.0.0.1/
REM sqlline.bat --verbose=true -u "jdbc:ignite:thin://127.0.0.1:10800;user=ignite;password=ignite"