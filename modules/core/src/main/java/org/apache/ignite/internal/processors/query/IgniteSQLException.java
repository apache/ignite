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

package org.apache.ignite.internal.processors.query;

import java.sql.SQLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.COLUMN_ALREADY_EXISTS;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.COLUMN_NOT_FOUND;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.CONCURRENT_UPDATE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.INDEX_ALREADY_EXISTS;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.INDEX_NOT_FOUND;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.KEY_UPDATE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.NULL_KEY;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.NULL_VALUE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.PARSING;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.STMT_TYPE_MISMATCH;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TABLE_ALREADY_EXISTS;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TABLE_NOT_FOUND;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.UNEXPECTED_ELEMENT_TYPE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.UNEXPECTED_OPERATION;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.UNKNOWN;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;

/**
 * Specific exception bearing information about query processing errors for more detailed
 * errors in JDBC driver.
 *
 * @see IgniteQueryErrorCode
 */
public class IgniteSQLException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** State to return as {@link SQLException#SQLState} */
    private final String sqlState;

    /** Code to return as {@link SQLException#vendorCode} */
    private final int statusCode;

    /** */
    public IgniteSQLException(String msg) {
        this(msg, UNKNOWN, null);
    }

    /**
     * Minimalistic ctor accepting only {@link SQLException} as the cause.
     */
    public IgniteSQLException(SQLException cause) {
        super(cause);
        this.sqlState = cause.getSQLState();
        this.statusCode = UNKNOWN;
    }

    /** */
    public IgniteSQLException(String msg, @Nullable Throwable cause) {
        this(msg, UNKNOWN, cause);
    }

    /** */
    public IgniteSQLException(String msg, int statusCode, @Nullable Throwable cause) {
        super(msg, cause);
        this.sqlState = (cause instanceof SQLException ? ((SQLException) cause).getSQLState() :
            codeToSqlState(statusCode));
        this.statusCode = statusCode;
    }

    /** */
    public IgniteSQLException(String msg, int statusCode) {
        super(msg);
        this.sqlState = codeToSqlState(statusCode);
        this.statusCode = statusCode;
    }

    /**
     * @return Ignite SQL error code.
     */
    public int statusCode() {
        return statusCode;
    }

    /**
     * @return JDBC exception containing details from this instance.
     */
    public SQLException toJdbcException() {
        return new SQLException(getMessage(), sqlState, statusCode, this);
    }

    /**
     * Map Ignite specific error code to standard SQL state.
     * @param statusCode Ignite specific error code.
     * @return SQL state string.
     * @see <a href="http://en.wikibooks.org/wiki/Structured_Query_Language/SQLSTATE">Wikipedia: SQLSTATE spec.</a>
     * @see IgniteQueryErrorCode
     */
    public static String codeToSqlState(int statusCode) {
        switch (statusCode) {
            case DUPLICATE_KEY:
                return "23000"; // Generic value for "integrity constraint violation" 23 class.

            case NULL_KEY:
            case NULL_VALUE:
                return "22004"; // "Null value not allowed".

            case UNSUPPORTED_OPERATION:
                return "0A000"; // Generic value for "feature not supported" 0A class.

            case CONCURRENT_UPDATE:
                return "40000"; // Generic value for "tx rollback" 40 class.

            case PARSING:
                return "42000"; // Generic value for "syntax error or access rule violation" 42 class.

            // 42 - class for "syntax error or access rule violation" + error specific part.
            case TABLE_NOT_FOUND:
                return "42001";

            case TABLE_ALREADY_EXISTS:
                return "42002";

            case INDEX_ALREADY_EXISTS:
                return "42003";

            case INDEX_NOT_FOUND:
                return "42004";

            case COLUMN_NOT_FOUND:
                return "42005";

            case COLUMN_ALREADY_EXISTS:
                return "42006";

            case STMT_TYPE_MISMATCH:
                return "42007";

            case UNEXPECTED_OPERATION:
                return "42008";

            case UNEXPECTED_ELEMENT_TYPE:
                return "42009";

            case KEY_UPDATE:
                return "42010";

            default:
                return "50000";  // Generic value for custom "50" class.
        }
    }
}
