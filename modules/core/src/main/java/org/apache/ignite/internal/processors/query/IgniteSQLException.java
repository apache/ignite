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
        this(msg, null, 0);
    }

    /**
     * Minimalistic ctor accepting only {@link SQLException} as the cause.
     */
    public IgniteSQLException(SQLException cause) {
        super(cause);
        this.sqlState = null;
        this.statusCode = 0;
    }

    /** */
    public IgniteSQLException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
        this.sqlState = null;
        this.statusCode = 0;
    }

    /** */
    public IgniteSQLException(String msg, int statusCode, @Nullable Throwable cause) {
        super(msg, cause);
        this.sqlState = null;
        this.statusCode = statusCode;
    }

    /** */
    public IgniteSQLException(String msg, String sqlState, int statusCode) {
        super(msg);
        this.sqlState = sqlState;
        this.statusCode = statusCode;
    }

    /** */
    public IgniteSQLException(String msg, int statusCode) {
        super(msg);
        this.sqlState = null;
        this.statusCode = statusCode;
    }

    /**
     * @return JDBC exception containing details from this instance.
     */
    public SQLException toJdbcException() {
        return new SQLException(getMessage(), sqlState, statusCode, this);
    }
}
