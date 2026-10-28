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

package org.apache.ignite.internal.processors.query.h2.twostep.messages;

import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.AbstractMessage;

/**
 * Error message.
 */
public class GridQueryFailResponse extends AbstractMessage {
    /** General error failure type. */
    public static final byte GENERAL_ERROR = 0;

    /** Cancelled by originator failure type. */
    public static final byte CANCELLED_BY_ORIGINATOR = 1;

    /** Execution error. Query should be retried. */
    public static final byte RETRY_QUERY = 2;

    /** */
    @Order(0)
    long qryReqId;

    /** */
    @Order(1)
    String errMsg;

    /** */
    @Order(2)
    byte failCode;

    /** Ignite SQL error status code, or {@code 0} if absent (for example, when the error came from an older node). */
    @Order(value = 3, introducedBy = "QUERY_FAIL_RESPONSE_ERROR_CODE_FEATURE")
    int statusCode;

    /**
     * Default constructor.
     */
    public GridQueryFailResponse() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param err Error.
     */
    public GridQueryFailResponse(long qryReqId, Throwable err) {
        this.qryReqId = qryReqId;
        errMsg = err.getMessage();

        if (err instanceof QueryCancelledException)
            failCode = CANCELLED_BY_ORIGINATOR;
        else if (err instanceof QueryRetryException)
            failCode = RETRY_QUERY;
        else
            failCode = GENERAL_ERROR;

        IgniteSQLException sqlErr = X.cause(err, IgniteSQLException.class);

        statusCode = sqlErr != null ? sqlErr.statusCode() : 0;
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return Error.
     */
    public String error() {
        return errMsg;
    }

    /**
     * @return Fail code.
     */
    public byte failCode() {
        return failCode;
    }

    /**
     * @return Ignite SQL error status code, or {@code 0} if it is absent.
     */
    public int statusCode() {
        return statusCode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryFailResponse.class, this);
    }

}
