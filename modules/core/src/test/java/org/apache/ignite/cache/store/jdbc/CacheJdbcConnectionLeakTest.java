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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.model.TestDelegatingConnection;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensures that there are no connection leaks when working with an external data source.
 */
public class CacheJdbcConnectionLeakTest extends GridCommonAbstractTest {
    /** Table name. */
    private static final String TABLE_NAME = "person";

    /** Connection listener. */
    private static final ConnectionListener connLsnr = new ConnectionListener();

    /** Datasource factory. */
    private static final Factory<DataSource> h2DsFactory = new H2DataSourceFactory();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(cacheConfig());
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration<Integer, Person> cacheConfig() {
        CacheConfiguration<Integer, Person> cacheConfig = new CacheConfiguration<Integer, Person>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setReadThrough(true)
            .setWriteThrough(true)
            .setIndexedTypes(Integer.class, Person.class)
            .setWriteBehindEnabled(true)
            .setWriteBehindFlushFrequency(1_000);

        CacheJdbcPojoStoreFactory<Integer, Person> cacheStoreFactory = new CacheJdbcPojoStoreFactory<>();

        cacheStoreFactory.setDataSourceFactory(() -> new DataSourceWrapper(h2DsFactory.create(), connLsnr))
            .setDialect(new H2Dialect())
            .setTypes(new JdbcType()
                .setCacheName(DEFAULT_CACHE_NAME)
                .setDatabaseTable(TABLE_NAME)
                .setKeyType(Integer.class)
                .setKeyFields(
                    new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"))
                .setValueType(Person.class)
                .setValueFields(
                    new JdbcTypeField(Types.VARCHAR, "name", String.class, "name"))
            );

        cacheConfig.setCacheStoreFactory(cacheStoreFactory);

        return cacheConfig;
    }

    /** */
    @Before
    public void setUp() throws Exception {
        String query = "CREATE TABLE " + TABLE_NAME + "(ID INT UNSIGNED PRIMARY KEY, NAME VARCHAR(100));";

        try (Connection connection = h2DsFactory.create().getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        }
    }

    /** */
    @Test
    public void testConnectionLeak() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {
            IgniteCache<Integer, Person> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            Person person = new Person(0);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.invoke(person.id, (entry, arg) -> {
                    Person newPerson = ((Person)arg[0]);

                    entry.setValue(newPerson);

                    return false;
                }, person);

                tx.commit();
            }
        }

        // We close ignite before assertion to be sure that
        // write-behind flushing is finished, and we can
        // safely proceed to connection open/close counting
        connLsnr.assertNotLeaked();
    }

    /** */
    public static class Person implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = 0;

        /** */
        @QuerySqlField
        private final Integer id;

        /** */
        @QuerySqlField
        private final String name;

        /**
         * @param id Person ID.
         */
        public Person(int id) {
            this.id = id;

            name = "Person_" + id;
        }
    }

    /** */
    static class ConnectionListener implements Serializable {
        /** Open connections counter. */
        private final AtomicLong openCntr = new AtomicLong();

        /** Closed connections counter. */
        private final AtomicLong closeCntr = new AtomicLong();

        /** */
        public void onOpen() {
            openCntr.incrementAndGet();
        }

        /** */
        public void onClose() {
            closeCntr.incrementAndGet();
        }

        /** */
        public void assertNotLeaked() {
            assertTrue(openCntr.get() > 0);
            assertEquals(openCntr.get(), closeCntr.get());
        }
    }

    /** */
    private static class DataSourceWrapper implements DataSource, Serializable {
        /** Datasource delegate. */
        private final DataSource delegate;

        /** Connection listener. */
        private final ConnectionListener connLsnr;

        /**
         * @param delegate Datasource delegate.
         * @param connLsnr Connection listener.
         */
        public DataSourceWrapper(DataSource delegate, ConnectionListener connLsnr) {
            this.delegate = delegate;
            this.connLsnr = connLsnr;
        }

        /** {@inheritDoc} */
        @Override public Connection getConnection() throws SQLException {
            connLsnr.onOpen();

            return new ConnectionWrapper(delegate.getConnection(), connLsnr);
        }

        /** {@inheritDoc} */
        @Override public Connection getConnection(String username, String password) throws SQLException {
            connLsnr.onOpen();

            return new ConnectionWrapper(delegate.getConnection(username, password), connLsnr);
        }

        /** {@inheritDoc} */
        @Override public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        /** {@inheritDoc} */
        @Override public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        /** {@inheritDoc} */
        @Override public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        /** {@inheritDoc} */
        @Override public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        /** {@inheritDoc} */
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        /** {@inheritDoc} */
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }

    /** */
    public static class ConnectionWrapper extends TestDelegatingConnection {
        /** Connection listener. */
        private final ConnectionListener connLsnr;

        /**
         * @param delegate Connection delegate.
         * @param connLsnr Connection listener.
         */
        public ConnectionWrapper(Connection delegate, ConnectionListener connLsnr) {
            this.delegate = delegate;
            this.connLsnr = connLsnr;
        }

        /** {@inheritDoc} */
        @Override public void close() throws SQLException {
            connLsnr.onClose();
            delegate.close();
        }
    }
}
