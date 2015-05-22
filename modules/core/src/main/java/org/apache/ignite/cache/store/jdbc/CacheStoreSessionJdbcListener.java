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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lifecycle.*;

import javax.cache.integration.*;
import javax.sql.*;
import java.sql.*;
import java.util.*;

/**
 * Cache store session listener based on JDBC connection.
 */
public class CacheStoreSessionJdbcListener implements CacheStoreSessionListener, LifecycleAware {
    /** Session key for JDBC connection. */
    public static final String JDBC_CONN_KEY = "__jdbc_conn_";

    /** Data source. */
    private DataSource dataSrc;

    /**
     * Sets data source.
     *
     * @param dataSrc Data source.
     */
    public void setDataSource(DataSource dataSrc) {
        A.notNull(dataSrc, "dataSrc");

        this.dataSrc = dataSrc;
    }

    /**
     * Gets data source.
     *
     * @return Data source.
     */
    public DataSource getDataSource() {
        return dataSrc;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (dataSrc == null)
            throw new IgniteException("Data source is required by " + getClass().getSimpleName() + '.');
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSessionStart(CacheStoreSession ses) {
        Map<String, Connection> props = ses.properties();

        if (!props.containsKey(JDBC_CONN_KEY)) {
            try {
                Connection conn = dataSrc.getConnection();

                conn.setAutoCommit(false);

                props.put(JDBC_CONN_KEY, conn);
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to start store session [tx=" + ses.transaction() + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
        Connection conn = ses.<String, Connection>properties().remove(JDBC_CONN_KEY);

        if (conn != null) {
            try {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to start store session [tx=" + ses.transaction() + ']', e);
            }
            finally {
                U.closeQuiet(conn);
            }
        }
    }
}
