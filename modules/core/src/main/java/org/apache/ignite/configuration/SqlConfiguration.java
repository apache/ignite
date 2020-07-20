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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * The configuration of the SQL query subsystem.
 */
public class SqlConfiguration {
    /** Default SQL query history size. */
    public static final int DFLT_SQL_QUERY_HISTORY_SIZE = 1000;

    /** Default query timeout. */
    public static final long DFLT_QRY_TIMEOUT = 0;

    /** Default timeout after which long query warning will be printed. */
    public static final long DFLT_LONG_QRY_WARN_TIMEOUT = 3000;

    /** */
    private long longQryWarnTimeout = DFLT_LONG_QRY_WARN_TIMEOUT;

    /** Default query timeout. */
    private long dfltQryTimeout = DFLT_QRY_TIMEOUT;

    /** SQL schemas to be created on node start. */
    private String[] sqlSchemas;

    /** SQL query history size. */
    private int sqlQryHistSize = DFLT_SQL_QUERY_HISTORY_SIZE;

    /** Enable validation of key & values against sql schema. */
    private boolean validationEnabled;

    /**
     * Defines the default query timeout.
     *
     * Defaults to {@link #DFLT_QRY_TIMEOUT}.
     * {@code 0} means there is no timeout (this is a default value)
     *
     * @return Default query timeout.
     */
    public long getDefaultQueryTimeout() {
        return dfltQryTimeout;
    }

    /**
     * Sets timeout in milliseconds for default query timeout.
     * {@code 0} means there is no timeout (this is a default value)
     *
     * @param dfltQryTimeout Timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setDefaultQueryTimeout(long dfltQryTimeout) {
        A.ensure(dfltQryTimeout >= 0 && dfltQryTimeout <= Integer.MAX_VALUE,
            "default query timeout value should be valid Integer.");

        this.dfltQryTimeout = dfltQryTimeout;

        return this;
    }

    /**
     * Number of SQL query history elements to keep in memory. If not provided, then default value {@link
     * #DFLT_SQL_QUERY_HISTORY_SIZE} is used. If provided value is less or equals 0, then gathering SQL query history
     * will be switched off.
     *
     * @return SQL query history size.
     */
    public int getSqlQueryHistorySize() {
        return sqlQryHistSize;
    }

    /**
     * Sets number of SQL query history elements kept in memory. If not explicitly set, then default value is {@link
     * #DFLT_SQL_QUERY_HISTORY_SIZE}.
     *
     * @param size Number of SQL query history elements kept in memory.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlQueryHistorySize(int size) {
        sqlQryHistSize = size;

        return this;
    }

    /**
     * Gets SQL schemas to be created on node startup.
     * <p>
     * See {@link #setSqlSchemas(String...)} for more information.
     *
     * @return SQL schemas to be created on node startup.
     */
    public String[] getSqlSchemas() {
        return sqlSchemas;
    }

    /**
     * Sets SQL schemas to be created on node startup. Schemas are created on local node only and are not propagated
     * to other cluster nodes. Created schemas cannot be dropped.
     * <p>
     * By default schema names are case-insensitive, i.e. {@code my_schema} and {@code My_Schema} represents the same
     * object. Use quotes to enforce case sensitivity (e.g. {@code "My_Schema"}).
     * <p>
     * Property is ignored if {@code ignite-indexing} module is not in classpath.
     *
     * @param sqlSchemas SQL schemas to be created on node startup.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlSchemas(String... sqlSchemas) {
        this.sqlSchemas = sqlSchemas;

        return this;
    }

    /**
     * Gets timeout in milliseconds after which long query warning will be printed.
     *
     * @return Timeout in milliseconds.
     */
    public long getLongQueryWarningTimeout() {
        return longQryWarnTimeout;
    }

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param longQryWarnTimeout Timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setLongQueryWarningTimeout(long longQryWarnTimeout) {
        this.longQryWarnTimeout = longQryWarnTimeout;

        return this;
    }

    /**
     * Is key & value validation enabled.
     *
     * @return {@code true} When key & value shall be validated against SQL schema.
     */
    public boolean isValidationEnabled() {
        return validationEnabled;
    }

    /**
     * Enable/disable key & value validation.
     *
     * @param validationEnabled {@code true} When key & value shall be validated against SQL schema.
     * Default value is {@code false}.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setValidationEnabled(boolean validationEnabled) {
        this.validationEnabled = validationEnabled;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlConfiguration.class, this);
    }
}
