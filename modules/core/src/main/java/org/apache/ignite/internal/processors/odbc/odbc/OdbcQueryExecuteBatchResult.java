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

import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute with batch of parameters result.
 */
public class OdbcQueryExecuteBatchResult {
    /** Rows affected. */
    private final long[] affectedRows;

    /** Index of the set which caused an error. */
    private final long errorSetIdx;

    /** Error code. */
    private final int errorCode;

    /** Error message. */
    private final String errorMessage;

    /**
     * @param affectedRows Number of rows affected by the query.
     */
    public OdbcQueryExecuteBatchResult(long[] affectedRows) {
        this.affectedRows = affectedRows;
        this.errorSetIdx = -1;
        this.errorMessage = null;
        this.errorCode = ClientListenerResponse.STATUS_SUCCESS;
    }

    /**
     * @param affectedRows Number of rows affected by the query.
     * @param errorSetIdx Sets processed.
     * @param errorCode Error code.
     * @param errorMessage Error message.
     */
    public OdbcQueryExecuteBatchResult(long[] affectedRows, long errorSetIdx, int errorCode,
        String errorMessage) {
        this.affectedRows = affectedRows;
        this.errorSetIdx = errorSetIdx;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }

    /**
     * @return Number of rows affected by the query.
     */
    public long[] affectedRows() {
        return affectedRows;
    }

    /**
     * @return Index of the set which caused an error or -1 if no error occurred.
     */
    public long errorSetIdx() {
        return errorSetIdx;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errorMessage;
    }

    /**
     * @return Error code.
     */
    public int errorCode() {
        return errorCode;
    }
}
