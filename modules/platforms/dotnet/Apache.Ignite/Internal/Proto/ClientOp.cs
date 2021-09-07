/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Proto
{
    /// <summary>
    /// Client operation codes.
    /// </summary>
    internal enum ClientOp
    {
        /** Create table. */
        TableCreate = 1,

        /** Drop table. */
        TableDrop = 2,

        /** Get tables. */
        TablesGet = 3,

        /** Get table. */
        TableGet = 4,

        /** Get schemas. */
        SchemasGet = 5,

        /** Upsert tuple. */
        TupleUpsert = 10,

        /** Upsert tuple without schema. */
        TupleUpsertSchemaless = 11,

        /** Get tuple. */
        TupleGet = 12,

        /** Upsert all tuples. */
        TupleUpsertAll = 13,

        /** Upsert all tuples without schema. */
        TupleUpsertAllSchemaless = 14,

        /** Get all tuples. */
        TupleGetAll = 15,

        /** Get and upsert tuple. */
        TupleGetAndUpsert = 16,

        /** Get and upsert tuple without schema. */
        TupleGetAndUpsertSchemaless = 17,

        /** Insert tuple. */
        TupleInsert = 18,

        /** Insert tuple without schema. */
        TupleInsertSchemaless = 19,

        /** Insert all tuples. */
        TupleInsertAll = 20,

        /** Insert all tuples without schema. */
        TupleInsertAllSchemaless = 21,

        /** Replace tuple. */
        TupleReplace = 22,

        /** Replace tuple without schema. */
        TupleReplaceSchemaless = 23,

        /** Replace exact tuple. */
        TupleReplaceExact = 24,

        /** Replace exact tuple without schema. */
        TupleReplaceExactSchemaless = 25,

        /** Get and replace tuple. */
        TupleGetAndReplace = 26,

        /** Get and replace tuple without schema. */
        TupleGetAndReplaceSchemaless = 27,

        /** Delete tuple. */
        TupleDelete = 28,

        /** Delete all tuples. */
        TupleDeleteAll = 29,

        /** Delete exact tuple. */
        TupleDeleteExact = 30,

        /** Delete all exact tuples. */
        TupleDeleteAllExact = 31,

        /** Get and delete tuple. */
        TupleGetAndDelete = 32,
    }
}
