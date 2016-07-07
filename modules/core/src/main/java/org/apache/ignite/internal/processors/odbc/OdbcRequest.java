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

package org.apache.ignite.internal.processors.odbc;

/**
 * ODBC command request.
 */
public class OdbcRequest {
    /** Handshake request. */
    public static final int HANDSHAKE = 1;

    /** Execute sql query. */
    public static final int EXECUTE_SQL_QUERY = 2;

    /** Fetch query results. */
    public static final int FETCH_SQL_QUERY = 3;

    /** Close query. */
    public static final int CLOSE_SQL_QUERY = 4;

    /** Get columns meta query. */
    public static final int GET_COLUMNS_META = 5;

    /** Get columns meta query. */
    public static final int GET_TABLES_META = 6;

    /** Command. */
    private final int cmd;

    /**
     * @param cmd Command type.
     */
    public OdbcRequest(int cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public int command() {
        return cmd;
    }
}
