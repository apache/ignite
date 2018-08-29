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

import java.util.Collection;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC streaming batch execute result.
 */
public class OdbcStreamingBatchResult {
    /** Rows affected. */
    private final Collection<Long> affectedRows;

    /** Error code. */
    private final int errorCode;

    /** Error message. */
    private final String errorMessage;

    /**
     * @param affectedRows Number of rows affected by the query.
     */
    public OdbcStreamingBatchResult(Collection<Long> affectedRows) {
        this.affectedRows = affectedRows;
        this.errorMessage = null;
        this.errorCode = ClientListenerResponse.STATUS_SUCCESS;
    }

    /**
     * @param affectedRows Number of rows affected by the query.
     * @param errorCode Error code.
     * @param errorMessage Error message.
     */
    public OdbcStreamingBatchResult(Collection<Long> affectedRows, int errorCode, String errorMessage) {
        this.affectedRows = affectedRows;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }

    /**
     * @return Number of rows affected by the query.
     */
    public Collection<Long> affectedRows() {
        return affectedRows;
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
