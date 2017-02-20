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

/**
 * Error codes for query operations.
 */
public final class IgniteQueryErrorCode {
    /** Unknown error, or the one without specific code. */
    public final static int UNKNOWN = 1;

    /* 1xxx - parsing errors */

    /** General parsing error - for the cases when there's no more specific code available. */
    public final static int PARSING = 1001;

    /** Code encountered unexpected type of SQL operation - like {@code EXPLAIN MERGE}. */
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

    /** Statement type does not match that declared by JDBC driver. */
    public final static int TABLE_DROP_FAILED = 3004;

    /* 4xxx - cache related runtime errors */

    /** Attempt to INSERT a key that is already in cache. */
    public final static int DUPLICATE_KEY = 4001;

    /** Attempt to UPDATE or DELETE a key whose value has been updated concurrently by someone else. */
    public final static int CONCURRENT_UPDATE = 4002;

    /** Attempt to INSERT or MERGE {@code null} key. */
    public final static int NULL_KEY = 4003;

    /** Attempt to INSERT or MERGE {@code null} value. */
    public final static int NULL_VALUE = 4004;

    /** {@link EntryProcessor} has thrown an exception during {@link IgniteCache#invokeAll}. */
    public final static int ENTRY_PROCESSING = 4005;

    /**
     * Create a {@link SQLException} for given code and message with null state.
     *
     * @param msg Message.
     * @param code Ignite status code.
     * @return {@link SQLException} with given details.
     */
    public static SQLException createJdbcSqlException(String msg, int code) {
        return new SQLException(msg, null, code);
    }
}
