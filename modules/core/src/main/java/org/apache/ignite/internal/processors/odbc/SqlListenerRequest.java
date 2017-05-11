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
 * SQL listener command request.
 */
public class SqlListenerRequest {
    /** Handshake request. */
    public static final int HANDSHAKE = 1;

    /** Execute sql query. */
    public static final int QRY_EXEC = 2;

    /** Fetch query results. */
    public static final int QRY_FETCH = 3;

    /** Close query. */
    public static final int QRY_CLOSE = 4;

    /** Get columns meta query. */
    public static final int META_COLS = 5;

    /** Get columns meta query. */
    public static final int META_TBLS = 6;

    /** Get parameters meta. */
    public static final int META_PARAMS = 7;

    /** Command. */
    private final int cmd;

    /** Request ID. */
    private long reqId;

    /**
     * @param cmd Command type.
     */
    public SqlListenerRequest(int cmd) {
        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public int command() {
        return cmd;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param reqId Request ID.
     */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }
}
