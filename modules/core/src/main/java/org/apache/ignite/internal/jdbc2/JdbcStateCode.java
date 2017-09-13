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

package org.apache.ignite.internal.jdbc2;

import java.sql.ResultSet;

/**
 * Error codes to throw from JDBC driver (SQLSTATE codes).
 */
public final class JdbcStateCode {
    /**
     * No-op constructor to prevent instantiation.
     */
    private JdbcStateCode() {
        // No-op.
    }

    /** General connection establishment error. */
    public final static String CONNECTION_ERROR = "08000";

    /** Client has failed to open connection with specified server. */
    public final static String CLIENT_CONNECTION_FAILED = "08001";

    /** Connection unexpectedly turned out to be in closed state. */
    public final static String CONNECTION_CLOSED = "08003";

    /** IO error during communication. */
    public final static String CONNECTION_FAILURE = "08006";

    /** Value can't be converted to expected type (to be used in {@link ResultSet}'s {@code getXxx()} methods. */
    public final static String DATA_EXCEPTION = "22000";

    /** Parameter type is not supported. */
    public final static String INVALID_PARAMETER_VALUE = "22023";

    /** Invalid result set state. */
    public final static String INVALID_CURSOR_STATE = "24000";

    /** Invalid cursor name. */
    public final static String INVALID_CURSOR_NAME = "34000";

    /** Invalid transaction level. */
    public final static String INVALID_TRANSACTION_LEVEL = "0700E";

    /** Invalid data type. */
    public final static String INVALID_DATA_TYPE = "0D000";

    /** Parsing exception. */
    public final static String PARSING_EXCEPTION = "42000";

    /** Internal error. */
    public final static String INTERNAL_ERROR = "50000";  // Generic value for custom "50" class.
}
