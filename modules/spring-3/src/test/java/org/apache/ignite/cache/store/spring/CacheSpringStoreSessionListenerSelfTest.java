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

package org.apache.ignite.cache.store.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.CacheStoreSessionListenerAbstractSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.TransactionStatus;

/**
 * Tests for {@link CacheJdbcStoreSessionListener}.
 */
public class CacheSpringStoreSessionListenerSelfTest extends CacheStoreSessionListenerAbstractSelfTest {
    /** */
    private static final DataSource DATA_SRC = new DriverManagerDataSource(URL);

    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheStore<Integer, Integer>> storeFactory() {
        return new Factory<CacheStore<Integer, Integer>>() {
            @Override public CacheStore<Integer, Integer> create() {
                return new Store(new JdbcTemplate(DATA_SRC));
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStoreSessionListener> sessionListenerFactory() {
        return new Factory<CacheStoreSessionListener>() {
            @Override public CacheStoreSessionListener create() {
                CacheSpringStoreSessionListener lsnr = new CacheSpringStoreSessionListener();

                lsnr.setDataSource(DATA_SRC);

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
        private final JdbcTemplate jdbc;

        /** */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /**
         * @param jdbc JDBC template.
         */
        private Store(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, Object... args) {
            loadCacheCnt.incrementAndGet();

            checkTransaction();
            checkConnection();
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            loadCnt.incrementAndGet();

            checkTransaction();
            checkConnection();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
            throws CacheWriterException {
            writeCnt.incrementAndGet();

            checkTransaction();
            checkConnection();

            if (write.get()) {
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

                jdbc.update("INSERT INTO " + table + " (key, value) VALUES (?, ?)",
                    entry.getKey(), entry.getValue());
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            deleteCnt.incrementAndGet();

            checkTransaction();
            checkConnection();
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            assertNull(ses.attachment());
        }

        /**
         */
        private void checkTransaction() {
            TransactionStatus tx = ses.attachment();

            if (ses.isWithinTransaction()) {
                assertNotNull(tx);
                assertFalse(tx.isCompleted());
            }
            else
                assertNull(tx);
        }

        /**
         */
        private void checkConnection() {
            Connection conn = DataSourceUtils.getConnection(jdbc.getDataSource());

            assertNotNull(conn);

            try {
                assertFalse(conn.isClosed());
                assertEquals(!ses.isWithinTransaction(), conn.getAutoCommit());
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