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

package org.apache.ignite.internal.jdbc;

import java.sql.SQLException;
import org.apache.ignite.internal.client.HostAndPortRange;

/**
 * Provide access and manipulations with connection JDBC properties.
 */
public interface ConnectionProperties {
    /**
     * Get the schema name.
     *
     * @return Schema name of the connection.
     */
    String getSchema();

    /**
     * Set the schema name.
     *
     * @param schema Schema name of the connection.
     */
    void setSchema(String schema);

    /**
     * Get the URL.
     *
     * @return The URL of the connection.
     */
    String getUrl();

    /**
     * Set the URL.
     *
     * @param url The URL of the connection.
     * @throws SQLException On invalid URL.
     */
    void setUrl(String url) throws SQLException;

    /**
     * Get the addresses.
     *
     * @return Ignite nodes addresses.
     */
    HostAndPortRange[] getAddresses();

    /**
     * Set the ignite node addresses.
     *
     * @param addrs Ignite nodes addresses.
     */
    void setAddresses(HostAndPortRange[] addrs);

    /**
     * Note: zero value means there is no limits.
     *
     * @return Query timeout in seconds.
     */
    Integer getQueryTimeout();

    /**
     * Note: zero value means there is no limits.
     *
     * @param qryTimeout Query timeout in seconds.
     * @throws SQLException On error.
     */
    void setQueryTimeout(Integer qryTimeout) throws SQLException;

    /**
     * Note: zero value means there is no limits.
     *
     * @return Retry limit.
     */
    Integer getRetryLimit();

    /**
     * Note: zero value means there is no limits.
     *
     * @param retryLimit Connection retry limit.
     * @throws SQLException On error.
     */
    void setRetryLimit(Integer retryLimit) throws SQLException;

    /**
     * Note: zero value means there is no limits.
     *
     * @return Reconnect throttling period.
     */
    Long getReconnectThrottlingPeriod();

    /**
     * Note: zero value means there is no limits.
     *
     * @param reconnectThrottlingPeriod Reconnect throttling period.
     * @throws SQLException On error.
     */
    void setReconnectThrottlingPeriod(Long reconnectThrottlingPeriod) throws SQLException;

    /**
     * Note: zero value means there is no limits.
     *
     * @return Reconnect throttling retries.
     */
    Integer getReconnectThrottlingRetries();

    /**
     * Note: zero value means there is no limits.
     *
     * @param ReconnectThrottlingRetries Reconnect throttling retries.
     * @throws SQLException On error.
     */
    void setReconnectThrottlingRetries(Integer ReconnectThrottlingRetries) throws SQLException;

    /**
     * Note: zero value means there is no limits.
     *
     * @return Connection timeout in milliseconds.
     */
    int getConnectionTimeout();

    /**
     * Note: zero value means there is no limits.
     *
     * @param connTimeout Connection timeout in milliseconds.
     * @throws SQLException On error.
     */
    void setConnectionTimeout(Integer connTimeout) throws SQLException;
}
