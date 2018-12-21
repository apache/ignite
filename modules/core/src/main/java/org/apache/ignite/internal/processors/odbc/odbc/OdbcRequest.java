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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.processors.odbc.ClientListenerRequestNoId;

/**
 * SQL listener command request.
 */
public class OdbcRequest extends ClientListenerRequestNoId {
    /** Execute sql query. */
    public static final byte QRY_EXEC = 2;

    /** Fetch query results. */
    public static final byte QRY_FETCH = 3;

    /** Close query. */
    public static final byte QRY_CLOSE = 4;

    /** Get columns meta query. */
    public static final byte META_COLS = 5;

    /** Get columns meta query. */
    public static final byte META_TBLS = 6;

    /** Get parameters meta. */
    public static final byte META_PARAMS = 7;

    /** Execute sql query with the batch of parameters. */
    public static final byte QRY_EXEC_BATCH = 8;

    /** Get next result set. */
    public static final byte MORE_RESULTS = 9;

    /** Process ordered streaming batch. */
    public static final byte STREAMING_BATCH = 10;

    /** Command. */
    private final byte cmd;

    /**
     * @param cmd Command type.
     */
    public OdbcRequest(byte cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public byte command() {
        return cmd;
    }
}
