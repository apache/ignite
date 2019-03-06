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

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * Cache store session listener based on JDBC connection.
 * <p>
 * For each session this listener gets a new JDBC connection
 * from provided {@link DataSource} and commits (or rolls
 * back) it when session ends.
 * <p>
 * The connection is saved as a store session
 * {@link CacheStoreSession#attachment() attachment}.
 * The listener guarantees that the connection will be
 * available for any store operation. If there is an
 * ongoing cache transaction, all operations within this
 * transaction will be committed or rolled back only when
 * the session ends.
 * <p>
 * As an example, here is how the {@link CacheStore#write(Cache.Entry)}
 * method can be implemented if {@link CacheJdbcStoreSessionListener}
 * is configured:
 * <pre name="code" class="java">
 * private static class Store extends CacheStoreAdapter&lt;Integer, Integer&gt; {
 *     &#64;CacheStoreSessionResource
 *     private CacheStoreSession ses;
 *
 *     &#64;Override public void write(Cache.Entry&lt;? extends Integer, ? extends Integer&gt; entry) throws CacheWriterException {
 *         // Get connection from the current session.
 *         Connection conn = ses.attachment();
 *
 *         // Execute update SQL query.
 *         try {
 *             conn.createStatement().executeUpdate("...");
 *         }
 *         catch (SQLException e) {
 *             throw new CacheWriterException("Failed to update the store.", e);
 *         }
 *     }
 * }
 * </pre>
 * JDBC connection will be automatically created by the listener
 * at the start of the session and closed when it ends.
 */
public class CacheJdbcStoreSessionListener implements CacheStoreSessionListener, LifecycleAware {
    /** Data source. */
    private DataSource dataSrc;

    /**
     * Sets data source.
     * <p>
     * This is a required parameter. If data source is not set,
     * exception will be thrown on startup.
     *
     * @param dataSrc Data source.
     */
    public void setDataSource(DataSource dataSrc) {
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
        if (ses.attachment() == null) {
            try {
                Connection conn = dataSrc.getConnection();

                conn.setAutoCommit(false);

                ses.attach(conn);
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to start store session [tx=" + ses.transaction() + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
        Connection conn = ses.attach(null);

        if (conn != null) {
            try {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to end store session [tx=" + ses.transaction() + ']', e);
            }
            finally {
                U.closeQuiet(conn);
            }
        }
    }
}
