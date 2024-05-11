@Echo Off 
:: 将当前目录设为默认目录 hankcs
title Hankcs's program
CD\ 
%~d0
CD %~dp0
java -Dc=one -Dtype=application/csv -jar post.jar *.csv
pause