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
public class OdbcStreamingBatchResult {
    /** Success status. */
    private int status;

    /** Error. */
    private String err;

    /** Order. */
    private final long order;

    /**
     * @param order Order.
     */
    public OdbcStreamingBatchResult(long order) {
        this(ClientListenerResponse.STATUS_SUCCESS, null, order);
    }

    /**
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     * @param order Order.
     */
    public OdbcStreamingBatchResult(int status, @Nullable String err, long order) {
        this.status = status;
        this.err = err;
        this.order = order;
    }

    /**
     * @return Success flag.
     */
    public int status() {
        return status;
    }

    /**
     * @return Error.
     */
    public String error() {
        return err;
    }

    /**
     * @return Order.
     */
    public long order() {
        return order;
    }
}
