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

package org.apache.ignite.internal.client.proto;

/**
 * Client operation codes.
 */
public class ClientOp {
    /** Create table. */
    public static final int TABLE_CREATE = 1;

    /** Drop table. */
    public static final int TABLE_DROP = 2;

    /** Get tables. */
    public static final int TABLES_GET = 3;

    /** Get table. */
    public static final int TABLE_GET = 4;

    /** Get schemas. */
    public static final int SCHEMAS_GET = 5;

    /** Upsert tuple. */
    public static final int TUPLE_UPSERT = 10;

    /** Upsert tuple without schema. */
    public static final int TUPLE_UPSERT_SCHEMALESS = 11;

    /** Get tuple. */
    public static final int TUPLE_GET = 12;

    /** Upsert all tuples. */
    public static final int TUPLE_UPSERT_ALL = 13;

    /** Upsert all tuples without schema. */
    public static final int TUPLE_UPSERT_ALL_SCHEMALESS = 14;

    /** Get all tuples. */
    public static final int TUPLE_GET_ALL = 15;

    /** Get and upsert tuple. */
    public static final int TUPLE_GET_AND_UPSERT = 16;

    /** Get and upsert tuple without schema. */
    public static final int TUPLE_GET_AND_UPSERT_SCHEMALESS = 17;

    /** Insert tuple. */
    public static final int TUPLE_INSERT = 18;

    /** Insert tuple without schema. */
    public static final int TUPLE_INSERT_SCHEMALESS = 19;

    /** Insert all tuples. */
    public static final int TUPLE_INSERT_ALL = 20;

    /** Insert all tuples without schema. */
    public static final int TUPLE_INSERT_ALL_SCHEMALESS = 21;

    /** Replace tuple. */
    public static final int TUPLE_REPLACE = 22;

    /** Replace tuple without schema. */
    public static final int TUPLE_REPLACE_SCHEMALESS = 23;

    /** Replace exact tuple. */
    public static final int TUPLE_REPLACE_EXACT = 24;

    /** Replace exact tuple without schema. */
    public static final int TUPLE_REPLACE_EXACT_SCHEMALESS = 25;

    /** Get and replace tuple. */
    public static final int TUPLE_GET_AND_REPLACE = 26;

    /** Get and replace tuple without schema. */
    public static final int TUPLE_GET_AND_REPLACE_SCHEMALESS = 27;

    /** Delete tuple. */
    public static final int TUPLE_DELETE = 28;

    /** Delete all tuples. */
    public static final int TUPLE_DELETE_ALL = 29;

    /** Delete exact tuple. */
    public static final int TUPLE_DELETE_EXACT = 30;

    /** Delete all exact tuples. */
    public static final int TUPLE_DELETE_ALL_EXACT = 31;

    /** Get and delete tuple. */
    public static final int TUPLE_GET_AND_DELETE = 32;

    /** Check if the tuple with the given key exists. */
    public static final int TUPLE_CONTAINS_KEY = 33;

    /** Execute sql query. */
    public static final int SQL_EXEC = 34;

    /** Get next batch by query cursor. */
    public static final int SQL_NEXT = 35;

    /** Execute batch query. */
    public static final int SQL_EXEC_BATCH = 36;

    /** Close query cursor. */
    public static final int SQL_CURSOR_CLOSE = 37;

    /** Get table metadata. */
    public static final int SQL_TABLE_META = 38;

    /** Get column metadata. */
    public static final int SQL_COLUMN_META = 39;

    /** Get schemas list. */
    public static final int SQL_SCHEMAS_META = 40;

    /** Get primary key metadata. */
    public static final int SQL_PK_META = 41;

    /** Get query metadata. */
    public static final int SQL_QUERY_META = 42;
}
