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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.CacheStoreSessionListenerAbstractSelfTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Before;

/**
 * Tests for {@link CacheJdbcStoreSessionListener}.
 */
public class CacheJdbcStoreSessionListenerSelfTest extends CacheStoreSessionListenerAbstractSelfTest {
    /** */
    @Before
    public void beforeCacheJdbcStoreSessionListenerSelfTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheStore<Integer, Integer>> storeFactory() {
        return new Factory<CacheStore<Integer, Integer>>() {
            @Override public CacheStore<Integer, Integer> create() {
                return new Store();
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStoreSessionListener> sessionListenerFactory() {
        return new Factory<CacheStoreSessionListener>() {
            @Override public CacheStoreSessionListener create() {
                CacheJdbcStoreSessionListener lsnr = new CacheJdbcStoreSessionListener();

                lsnr.setDataSource(JdbcConnectionPool.create(URL, "", ""));

                return lsnr;
            }
        };
    }

    /**
     */
    private static class Store extends CacheStoreAdapter<Integer, Integer> {
        /** */
        private static String SES_CONN_KEY = "ses_conn";

        /** */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, Object... args) {
            loadCacheCnt.incrementAndGet();

            checkConnection();
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            loadCnt.incrementAndGet();

            checkConnection();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
            throws CacheWriterException {
            writeCnt.incrementAndGet();

            checkConnection();

            if (write.get()) {
                Connection conn = ses.attachment();

                try {
                    String table;

                    switch (ses.cacheName()) {
                        case "cache1":
                            table = "Table1";

                            break;

                        case "cache2":
                            if (fail.get())
                                throw new CacheWriterException("Expected failure.");

                            table = "Table2";

                            break;

                        default:
                            throw new CacheWriterException("Wring cache: " + ses.cacheName());
                    }

                    PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO " + table + " (key, value) VALUES (?, ?)");

                    stmt.setInt(1, entry.getKey());
                    stmt.setInt(2, entry.getValue());

                    stmt.executeUpdate();
                }
                catch (SQLException e) {
                    throw new CacheWriterException(e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            deleteCnt.incrementAndGet();

            checkConnection();
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            assertNull(ses.attachment());
        }

        /**
         */
        private void checkConnection() {
            Connection conn = ses.attachment();

            assertNotNull(conn);

            try {
                assertFalse(conn.isClosed());
                assertFalse(conn.getAutoCommit());
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }

            verifySameInstance(conn);
        }

        /**
         * @param conn Connection.
         */
        private void verifySameInstance(Connection conn) {
            Map<String, Connection> props = ses.properties();

            Connection sesConn = props.get(SES_CONN_KEY);

            if (sesConn == null)
                props.put(SES_CONN_KEY, conn);
            else {
                assertSame(conn, sesConn);

                reuseCnt.incrementAndGet();
            }
        }
    }
}
