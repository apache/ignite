@IF EXIST "%~dp0\node.exe" (
  "%~dp0\node.exe"  "%~dp0\..\postcss-svgo\node_modules\svgo\bin\svgo" %*
) ELSE (
  @SETLOCAL
  @SET PATHEXT=%PATHEXT:;.JS;=;%
  node  "%~dp0\..\postcss-svgo\node_modules\svgo\bin\svgo" %*
)