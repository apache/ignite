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

package org.apache.ignite.client.proto;

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
}
