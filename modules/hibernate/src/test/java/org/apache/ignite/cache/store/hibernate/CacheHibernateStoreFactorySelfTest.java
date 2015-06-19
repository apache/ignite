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

package org.apache.ignite.cache.store.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.hibernate.*;
import org.hibernate.engine.spi.*;
import org.hibernate.metadata.*;
import org.hibernate.stat.*;

import javax.naming.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Test for Cache jdbc blob store factory.
 */
public class CacheHibernateStoreFactorySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /**
     * @throws Exception If failed.
     */
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignite1 = startGrid(0)) {
            IgniteCache<Integer, String> cache1 = ignite1.getOrCreateCache(cacheConfiguration());

            checkStore(cache1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testXmlConfiguration() throws Exception {
        try (Ignite ignite = Ignition.start("modules/hibernate/src/test/config/factory-cache.xml")) {
            try(Ignite ignite1 = Ignition.start("modules/hibernate/src/test/config/factory-cache1.xml")) {
                checkStore(ignite.<Integer, String>cache(CACHE_NAME), DummySessionFactoryExt.class);

                checkStore(ignite1.<Integer, String>cache(CACHE_NAME), DummySessionFactory.class);
            }
        }
    }


    /**
     * @throws Exception If failed.
     */
    public void testIncorrectBeanConfiguration() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try(Ignite ignite =
                    Ignition.start("modules/hibernate/src/test/config/factory-incorrect-store-cache.xml")) {
                    ignite.cache(CACHE_NAME).getConfiguration(CacheConfiguration.class).
                            getCacheStoreFactory().create();
                }
                return null;
            }
        }, IgniteException.class, "Failed to load bean in application context");
    }

    /**
     * @return Cache configuration with store.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        CacheHibernateBlobStoreFactory<Integer, String> factory = new CacheHibernateBlobStoreFactory();

        factory.setHibernateConfigurationPath("/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        cfg.setCacheStoreFactory(factory);

        return cfg;
    }

    /**
     * @param cache Ignite cache.
     * @param dataSrcClass Data source class.
     * @throws Exception If store parameters is not the same as in configuration xml.
     */
    private void checkStore(IgniteCache<Integer, String> cache, Class<?> dataSrcClass) throws Exception {
        CacheHibernateBlobStore store = (CacheHibernateBlobStore)cache
            .getConfiguration(CacheConfiguration.class).getCacheStoreFactory().create();

        assertEquals(dataSrcClass,
            GridTestUtils.getFieldValue(store, CacheHibernateBlobStore.class, "sesFactory").getClass());
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If store parameters is not the same as in configuration xml.
     */
    private void checkStore(IgniteCache<Integer, String> cache) throws Exception {
        CacheHibernateBlobStore store = (CacheHibernateBlobStore)cache.getConfiguration(CacheConfiguration.class)
            .getCacheStoreFactory().create();

        assertEquals("/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml",
            GridTestUtils.getFieldValue(store, CacheHibernateBlobStore.class, "hibernateCfgPath"));
    }

    /**
     *
     */
    public static class DummySessionFactoryExt extends DummySessionFactory {
        /** */
        public DummySessionFactoryExt() {
            // No-op.
        }
    }

    /**
     *
     */
    public static class DummySessionFactory implements SessionFactory {
        /** {@inheritDoc} */
        @Override public SessionFactoryOptions getSessionFactoryOptions() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public SessionBuilder withOptions() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Session openSession() throws HibernateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Session getCurrentSession() throws HibernateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public StatelessSessionBuilder withStatelessOptions() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public StatelessSession openStatelessSession() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public StatelessSession openStatelessSession(Connection connection) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClassMetadata getClassMetadata(Class entityClass) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClassMetadata getClassMetadata(String entityName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public CollectionMetadata getCollectionMetadata(String roleName) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<String, ClassMetadata> getAllClassMetadata() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map getAllCollectionMetadata() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Statistics getStatistics() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void close() throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public boolean isClosed() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Cache getCache() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void evict(Class persistentClass) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evict(Class persistentClass, Serializable id) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evictEntity(String entityName) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evictEntity(String entityName, Serializable id) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evictCollection(String roleName) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evictCollection(String roleName, Serializable id) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evictQueries(String cacheRegion) throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public void evictQueries() throws HibernateException {
        }

        /** {@inheritDoc} */
        @Override public Set getDefinedFilterNames() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FilterDefinition getFilterDefinition(String filterName) throws HibernateException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean containsFetchProfileDefinition(String name) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public TypeHelper getTypeHelper() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Reference getReference() throws NamingException {
            return null;
        }
    }
}
