/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query;

import java.sql.SQLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.UNKNOWN;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.codeToSqlState;

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

    /**
     * Constructor.
     *
     * @param msg Exception message.
     */
    public IgniteSQLException(String msg) {
        this(msg, UNKNOWN, (String)null);
    }

    /**
     * Constructor.
     *
     * @param cause Cause to throw this exception.
     */
    public IgniteSQLException(SQLException cause) {
        super(cause);

        this.sqlState = cause.getSQLState();
        this.statusCode = UNKNOWN;
    }

    /**
     * Constructor.
     *
     * @param msg Exception message.
     * @param cause Cause to throw this exception.
     */
    public IgniteSQLException(String msg, @Nullable Throwable cause) {
        this(msg, UNKNOWN, cause);
    }

    /**
     * Constructor.
     *
     * @param msg Exception message.
     * @param statusCode Ignite specific error code.
     * @param cause Cause to throw this exception.
     * @see IgniteQueryErrorCode
     */
    public IgniteSQLException(String msg, int statusCode, @Nullable Throwable cause) {
        this(msg, statusCode, codeToSqlState(statusCode), cause);
    }

    /**
     * Constructor.
     * @param msg Exception message.
     * @param statusCode Ignite specific error code.
     * @see IgniteQueryErrorCode
     */
    public IgniteSQLException(String msg, int statusCode) {
        this(msg, statusCode, codeToSqlState(statusCode));
    }

    /**
     * Constructor.
     * @param msg Exception message.
     * @param statusCode Ignite specific error code.
     * @param sqlState SQLSTATE standard code.
     * @see IgniteQueryErrorCode
     */
    public IgniteSQLException(String msg, int statusCode, String sqlState) {
        this(msg, statusCode, sqlState, null);
    }

    /**
     * Constructor.
     * @param msg Exception message.
     * @param statusCode Ignite specific error code.
     * @param sqlState SQLSTATE standard code.
     * @param cause Cause to throw this exception.
     * @see IgniteQueryErrorCode
     */
    private IgniteSQLException(String msg, int statusCode, String sqlState, @Nullable Throwable cause) {
        super(msg, cause);

        this.sqlState = sqlState;
        this.statusCode = statusCode;
    }

    /**
     * @return Ignite SQL error code.
     */
    public int statusCode() {
        return statusCode;
    }

    /**
     * {@link SQLException#SQLState} getter.
     *
     * @return {@link SQLException#SQLState}.
     */
    public String sqlState() {
        return sqlState;
    }

    /**
     * @return JDBC exception containing details from this instance.
     */
    public SQLException toJdbcException() {
        return new SQLException(getMessage(), sqlState, statusCode, this);
    }
}
