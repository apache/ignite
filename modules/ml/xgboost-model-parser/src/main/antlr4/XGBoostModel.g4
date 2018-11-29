/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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