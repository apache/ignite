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
    public static final int UNKNOWN = 1;

    /* 1xxx - parsing errors */

    /** General parsing error - for the cases when there's no more specific code available. */
    public static final int PARSING = 1001;

    /** Requested operation is not supported. */
    public static final int UNSUPPORTED_OPERATION = 1002;

    /* 2xxx - analysis errors */

    /** Code encountered SQL statement of some type that it did not expect in current analysis context. */
    public static final int UNEXPECTED_OPERATION = 2001;

    /** Code encountered SQL expression of some type that it did not expect in current analysis context. */
    public static final int UNEXPECTED_ELEMENT_TYPE = 2002;

    /** Analysis detected that the statement is trying to directly {@code UPDATE} key or its fields. */
    public static final int KEY_UPDATE = 2003;

    /* 3xxx - database API related runtime errors */

    /** Required table not found. */
    public static final int TABLE_NOT_FOUND = 3001;

    /** Required table does not have a descriptor set. */
    public static final int NULL_TABLE_DESCRIPTOR = 3002;

    /** Statement type does not match that declared by JDBC driver. */
    public static final int STMT_TYPE_MISMATCH = 3003;

    /** DROP TABLE failed. */
    public static final int TABLE_DROP_FAILED = 3004;

    /** Index already exists. */
    public static final int INDEX_ALREADY_EXISTS = 3005;

    /** Index does not exist. */
    public static final int INDEX_NOT_FOUND = 3006;

    /** Required table already exists. */
    public static final int TABLE_ALREADY_EXISTS = 3007;

    /** Required column not found. */
    public static final int COLUMN_NOT_FOUND = 3008;

    /** Required column already exists. */
    public static final int COLUMN_ALREADY_EXISTS = 3009;

    /** Conversion failure. */
    public static final int CONVERSION_FAILED = 3013;

    /** Query canceled. */
    public static final int QUERY_CANCELED = 3014;

    /* 4xxx - cache related runtime errors */

    /** Attempt to INSERT a key that is already in cache. */
    public static final int DUPLICATE_KEY = 4001;

    /** Attempt to UPDATE or DELETE a key whose value has been updated concurrently by someone else. */
    public static final int CONCURRENT_UPDATE = 4002;

    /** Attempt to INSERT or MERGE {@code null} key. */
    public static final int NULL_KEY = 4003;

    /** Attempt to INSERT or MERGE {@code null} value, or to to set {@code null} to a {@code NOT NULL} column. */
    public static final int NULL_VALUE = 4004;

    /** {@link EntryProcessor} has thrown an exception during {@link IgniteCache#invokeAll}. */
    public static final int ENTRY_PROCESSING = 4005;

    /** Cache not found. */
    public static final int CACHE_NOT_FOUND = 4006;

    /** Attempt to INSERT, UPDATE or MERGE key that exceed maximum column length. */
    public static final int TOO_LONG_KEY = 4007;

    /** Attempt to INSERT, UPDATE or MERGE value that exceed maximum column length. */
    public static final int TOO_LONG_VALUE = 4008;

    /** Attempt to INSERT, UPDATE or MERGE value which scale exceed maximum DECIMAL column scale. */
    public static final int VALUE_SCALE_OUT_OF_RANGE = 4009;

    /** Attempt to INSERT, UPDATE or MERGE value which scale exceed maximum DECIMAL column scale. */
    public static final int KEY_SCALE_OUT_OF_RANGE = 4010;

    /** Attempt to INSERT, UPDATE or DELETE value on read-only cluster. */
    public static final int CLUSTER_READ_ONLY_MODE_ENABLED = 4011;

    /* 5xxx - transactions related runtime errors. */

    /** Transaction is already open. */
    public static final int TRANSACTION_EXISTS = 5001;

    /** MVCC disabled. */
    public static final int MVCC_DISABLED = 5002;

    /** Transaction type mismatch (SQL/non SQL). */
    public static final int TRANSACTION_TYPE_MISMATCH = 5003;

    /** Transaction is already completed. */
    public static final int TRANSACTION_COMPLETED = 5004;

    /** Transaction serialization error. */
    public static final int TRANSACTION_SERIALIZATION_ERROR = 5005;

    /** Field type mismatch. e.g.: cause is {@link ClassCastException}. */
    public static final int FIELD_TYPE_MISMATCH = 5006;

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
            case KEY_SCALE_OUT_OF_RANGE:
            case VALUE_SCALE_OUT_OF_RANGE:
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

            case TRANSACTION_SERIALIZATION_ERROR:
                return SqlStateCode.SERIALIZATION_FAILURE;

            case QUERY_CANCELED:
                return SqlStateCode.QUERY_CANCELLED;

            case CLUSTER_READ_ONLY_MODE_ENABLED:
                return SqlStateCode.CLUSTER_READ_ONLY_MODE_ENABLED;

            default:
                return SqlStateCode.INTERNAL_ERROR;
        }
    }
}
