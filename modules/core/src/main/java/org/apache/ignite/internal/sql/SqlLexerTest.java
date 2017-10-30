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

package org.apache.ignite.internal.sql;

public class SqlLexerTest {
    public static void main(String[] args) {
//        SqlLexer lex = new SqlLexer("CREATE INDEX a_idx ON PUBLIC.\"tEst\" (col1 ASC, COl2, \"coooL4\")");
//
//        while (lex.shift())
//            System.out.println(lex.token() + " -> " + lex.tokenType());


        //System.out.println(new SqlParser("DROP INDEX IF EXISTS a.a;").nextCommand());

        System.out.println(new SqlParser("CREATE TABLE t (a VARCHAR, b TINYINT, PRIMARY KEY (a))").nextCommand());
    }
}
