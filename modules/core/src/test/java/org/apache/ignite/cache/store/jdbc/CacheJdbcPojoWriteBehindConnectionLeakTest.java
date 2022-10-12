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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensures that there are no connection leaks when working with an external data source.
 */
public class CacheJdbcPojoWriteBehindConnectionLeakTest extends GridCommonAbstractTest {
    /** Table name. */
    private static final String TABLE_NAME = "person";

    /** Connection pool. */
    private static final JdbcConnectionPool pool = (JdbcConnectionPool)new H2DataSourceFactory().create();

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
            .setWriteBehindEnabled(true)
            .setWriteBehindFlushFrequency(1_000);

        CacheJdbcPojoStoreFactory<Integer, Person> cacheStoreFactory = new CacheJdbcPojoStoreFactory<>();

        cacheStoreFactory.setDataSourceFactory(() -> pool)
            .setDialect(new H2Dialect())
            .setTypes(new JdbcType()
                .setCacheName(DEFAULT_CACHE_NAME)
                .setDatabaseTable(TABLE_NAME)
                .setKeyType(Integer.class)
                .setKeyFields(new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"))
                .setValueType(Person.class)
                .setValueFields(new JdbcTypeField(Types.VARCHAR, "name", String.class, "name"))
            );

        cacheConfig.setCacheStoreFactory(cacheStoreFactory);

        return cacheConfig;
    }

    /** */
    @Before
    public void setUp() throws SQLException {
        execStandaloneQuery("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(ID INT UNSIGNED PRIMARY KEY, NAME VARCHAR(20))");
    }

    /** */
    @After
    public void tearDown() throws SQLException {
        execStandaloneQuery("DROP TABLE " + TABLE_NAME);
    }

    /** */
    @Test
    public void testInvoke() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            IgniteCache<Integer, Person> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.invoke(0, (entry, arg) -> true);

                tx.commit();
            }
        }

        // We close ignite before assertion to be sure that write-behind flushing is finished.
        assertEquals(0, pool.getActiveConnections());
    }

    /**
     * @param sql SQL query.
     * @throws SQLException If failed.
     */
    private void execStandaloneQuery(String sql) throws SQLException {
        try (Connection connection = pool.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    /** */
    public static class Person implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = 0;

        /** */
        private Integer id;

        /** */
        private String name;
    }
}
