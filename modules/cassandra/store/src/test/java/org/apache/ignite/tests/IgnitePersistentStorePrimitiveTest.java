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

package org.apache.ignite.tests;

import java.io.IOException;
import java.net.URL;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.cassandra.CassandraCacheStoreFactory;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.tests.utils.CassandraAdminCredentials;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store primitive type cache data into Cassandra table.
 */
public class IgnitePersistentStorePrimitiveTest {
    /** */
    private static final Logger LOGGER = Logger.getLogger(IgnitePersistentStorePrimitiveTest.class.getName());

    /** */
    @BeforeClass
    public static void setUpClass() {
        if (CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }

        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();

        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();

        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();

        LOGGER.info("Start tests execution");
    }

    /** */
    @AfterClass
    public static void tearDownClass() {
        try {
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();

            if (CassandraHelper.useEmbeddedCassandra()) {
                try {
                    CassandraHelper.stopEmbeddedCassandra();
                }
                catch (Throwable e) {
                    LOGGER.error("Failed to stop embedded Cassandra instance", e);
                }
            }
        }
    }

    /** */
    @Test
    public void test() throws IOException {
        IgniteConfiguration config = igniteConfig();

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start(config)) {
            IgniteCache<Long, Long> cache = ignite.getOrCreateCache("cache1");
            cache.put(12L, 12L);
        }

        Ignition.stopAll(true);

        try (Ignite ignite = Ignition.start(config)) {
            IgniteCache<Long, Long> cache = ignite.getOrCreateCache("cache1");

            assertEquals(12L, (long)cache.get(12L));

            cache.remove(12L);
        }
    }

    /** */
    private IgniteConfiguration igniteConfig() throws IOException {
        URL url = getClass().getClassLoader().getResource("org/apache/ignite/tests/persistence/blob/persistence-settings-1.xml");
        String persistence = U.readFileToString(url.getFile(), "UTF-8");
        KeyValuePersistenceSettings persistenceSettings = new KeyValuePersistenceSettings(persistence);

        DataSource dataSource = new DataSource();
        dataSource.setContactPoints(CassandraHelper.getContactPointsArray());
        dataSource.setCredentials(new CassandraAdminCredentials());
        dataSource.setLoadBalancingPolicy(new RoundRobinPolicy());

        CassandraCacheStoreFactory<Long, Long> storeFactory = new CassandraCacheStoreFactory<>();
        storeFactory.setDataSource(dataSource);
        storeFactory.setPersistenceSettings(persistenceSettings);

        CacheConfiguration<Long, Long> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("cache1");
        cacheConfiguration.setReadThrough(true);
        cacheConfiguration.setWriteThrough(true);
        cacheConfiguration.setCacheStoreFactory(storeFactory);

        IgniteConfiguration config = new IgniteConfiguration();
        config.setCacheConfiguration(cacheConfiguration);

        return config;
    }
}
