grammar XGBoostModel;

@header {
package org.apache.ignite.ml.xgboost.parser;
}

YES : 'yes' ;
NO : 'no' ;
MISSING : 'missing' ;
EQ : '=' ;
COMMA : ',' ;
PLUS : '+' ;
MINUS : '-' ;
DOT : '.' ;
EXP : 'E' | 'e' ;
BOOSTER : 'booster' ;
LBRACK : '[' ;
RBRACK : ']' ;
COLON : ':' ;
LEAF : 'leaf' ;
INT : (PLUS | MINUS)? [0-9]+ ;
DOUBLE : INT DOT [0-9]* (EXP INT)?;
STRING : [A-Za-z_][0-9A-Za-z_]+ ;
NEWLINE : '\r' '\n' | '\n' | '\r' ;
LT : '<' ;
WS : ( ' ' | '\t' )+ -> skip ;

xgValue : DOUBLE | INT ;
xgHeader : BOOSTER LBRACK INT RBRACK COLON? ;
xgNode : INT COLON LBRACK STRING LT xgValue RBRACK YES EQ INT COMMA NO EQ INT COMMA MISSING EQ INT ;
xgLeaf : INT COLON LEAF EQ xgValue ;
xgTree : xgHeader NEWLINE (
    ((xgLeaf | xgNode) NEWLINE)+ ((xgLeaf | xgNode) EOF)?
    | ((xgLeaf | xgNode) NEWLINE)* (xgLeaf | xgNode) EOF
) ;
xgModel : xgTree+ ;