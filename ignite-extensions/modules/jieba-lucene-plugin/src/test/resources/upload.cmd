@Echo Off 
:: ����ǰĿ¼��ΪĬ��Ŀ¼ hankcs
title Hankcs's program
CD\ 
%~d0
CD %~dp0
java -Dc=one -Dtype=application/csv -jar post.jar *.csv
pause