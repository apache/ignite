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

package org.apache.ignite.client.proto.query;

import java.sql.SQLException;

/**
 * Error codes for query operations.
 */
public final class IgniteQueryErrorCode {
    /** Unknown error, or the one without specific code. */
    public static final int UNKNOWN = 1;

    /* 1xxx - parsing errors */

    /** General parsing error - for the cases when there's no more specific code available. */
    public static final int PARSING = 1001;

    /** Requested operation is not supported. */
    public static final int UNSUPPORTED_OPERATION = 1002;

    /* 2xxx - analysis errors */
    /* 3xxx - database API related runtime errors */
    /* 4xxx - cache related runtime errors */
    /* 5xxx - transactions related runtime errors. */

    /**
     * Constructor.
     */
    private IgniteQueryErrorCode() {
        // No-op.
    }

    /**
     * Create a {@link SQLException} for given code and message with detected state.
     *
     * @param msg  Message.
     * @param code Ignite status code.
     * @return {@link SQLException} with given details.
     */
    public static SQLException createJdbcSqlException(String msg, int code) {
        return new SQLException(msg, codeToSqlState(code));
    }

    /**
     * Map Ignite specific error code to standard SQL state.
     *
     * @param statusCode Ignite specific error code.
     * @return SQL state string.
     * @see <a href="http://en.wikibooks.org/wiki/Structured_Query_Language/SQLSTATE">Wikipedia: SQLSTATE spec.</a>
     * @see IgniteQueryErrorCode
     */
    public static String codeToSqlState(int statusCode) {
        switch (statusCode) {
            case UNSUPPORTED_OPERATION:
                return SqlStateCode.UNSUPPORTED_OPERATION;

            case PARSING:
                return SqlStateCode.PARSING_EXCEPTION;

            default:
                return SqlStateCode.INTERNAL_ERROR;
        }
    }
}
