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

package org.apache.ignite.internal.processors.cache.query;

import java.sql.SQLException;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;

/**
 * Error codes for query operations.
 */
public final class IgniteQueryErrorCode {
    /** Unknown error, or the one without specific code. */
    public final static int UNKNOWN = 1;

    /* 1xxx - parsing errors */

    /** General parsing error - for the cases when there's no more specific code available. */
    public final static int PARSING = 1001;

    /** Requested operation is not supported. */
    public final static int UNSUPPORTED_OPERATION = 1002;

    /* 2xxx - analysis errors */

    /** Code encountered SQL statement of some type that it did not expect in current analysis context. */
    public final static int UNEXPECTED_OPERATION = 2001;

    /** Code encountered SQL expression of some type that it did not expect in current analysis context. */
    public final static int UNEXPECTED_ELEMENT_TYPE = 2002;

    /** Analysis detected that the statement is trying to directly {@code UPDATE} key or its fields. */
    public final static int KEY_UPDATE = 2003;

    /* 3xxx - database API related runtime errors */

    /** Required table not found. */
    public final static int TABLE_NOT_FOUND = 3001;

    /** Required table does not have a descriptor set. */
    public final static int NULL_TABLE_DESCRIPTOR = 3002;

    /** Statement type does not match that declared by JDBC driver. */
    public final static int STMT_TYPE_MISMATCH = 3003;

    /** DROP TABLE failed. */
    public final static int TABLE_DROP_FAILED = 3004;

    /** Index already exists. */
    public final static int INDEX_ALREADY_EXISTS = 3005;

    /** Index does not exist. */
    public final static int INDEX_NOT_FOUND = 3006;

    /** Required table already exists. */
    public final static int TABLE_ALREADY_EXISTS = 3007;

    /** Required column not found. */
    public final static int COLUMN_NOT_FOUND = 3008;

    /** Required column already exists. */
    public final static int COLUMN_ALREADY_EXISTS = 3009;

    /** Conversion failure. */
    public final static int CONVERSION_FAILED = 3013;

    /* 4xxx - cache related runtime errors */

    /** Attempt to INSERT a key that is already in cache. */
    public final static int DUPLICATE_KEY = 4001;

    /** Attempt to UPDATE or DELETE a key whose value has been updated concurrently by someone else. */
    public final static int CONCURRENT_UPDATE = 4002;

    /** Attempt to INSERT or MERGE {@code null} key. */
    public final static int NULL_KEY = 4003;

    /** Attempt to INSERT or MERGE {@code null} value, or to to set {@code null} to a {@code NOT NULL} column. */
    public final static int NULL_VALUE = 4004;

    /** {@link EntryProcessor} has thrown an exception during {@link IgniteCache#invokeAll}. */
    public final static int ENTRY_PROCESSING = 4005;

    /** Cache not found. */
    public final static int CACHE_NOT_FOUND = 4006;

    /** Attempt to INSERT, UPDATE or MERGE key that exceed maximum column length. */
    public final static int TOO_LONG_KEY = 4007;

    /** Attempt to INSERT, UPDATE or MERGE value that exceed maximum column length. */
    public final static int TOO_LONG_VALUE = 4008;

    /* 5xxx - transactions related runtime errors. */

    /** Transaction is already open. */
    public final static int TRANSACTION_EXISTS = 5001;

    /** MVCC disabled. */
    public final static int MVCC_DISABLED = 5002;

    /** Transaction type mismatch (SQL/non SQL). */
    public final static int TRANSACTION_TYPE_MISMATCH = 5003;

    /** Transaction is already completed. */
    public final static int TRANSACTION_COMPLETED = 5004;

    /** */
    private IgniteQueryErrorCode() {
        // No-op.
    }

    /**
     * Create a {@link SQLException} for given code and message with detected state.
     *
     * @param msg Message.
     * @param code Ignite status code.
     * @return {@link SQLException} with given details.
     */
    public static SQLException createJdbcSqlException(String msg, int code) {
        return new SQLException(msg, codeToSqlState(code));
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
            case TOO_LONG_KEY:
            case TOO_LONG_VALUE:
                return SqlStateCode.CONSTRAINT_VIOLATION;

            case NULL_KEY:
            case NULL_VALUE:
                return SqlStateCode.NULL_VALUE;

            case UNSUPPORTED_OPERATION:
                return SqlStateCode.UNSUPPORTED_OPERATION;

            case CONVERSION_FAILED:
                return SqlStateCode.CONVERSION_FAILED;

            case PARSING:
            case TABLE_NOT_FOUND:
            case TABLE_ALREADY_EXISTS:
            case INDEX_ALREADY_EXISTS:
            case INDEX_NOT_FOUND:
            case COLUMN_NOT_FOUND:
            case COLUMN_ALREADY_EXISTS:
            case STMT_TYPE_MISMATCH:
            case UNEXPECTED_OPERATION:
            case UNEXPECTED_ELEMENT_TYPE:
            case KEY_UPDATE:
                return SqlStateCode.PARSING_EXCEPTION;

            case MVCC_DISABLED:
            case TRANSACTION_EXISTS:
            case TRANSACTION_TYPE_MISMATCH:
            case TRANSACTION_COMPLETED:
                return SqlStateCode.TRANSACTION_STATE_EXCEPTION;

            default:
                return SqlStateCode.INTERNAL_ERROR;
        }
    }
}
