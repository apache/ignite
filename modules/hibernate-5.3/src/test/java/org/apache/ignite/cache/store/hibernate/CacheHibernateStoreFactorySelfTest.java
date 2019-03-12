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

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.SynchronizationType;
import javax.persistence.criteria.CriteriaBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hibernate.Cache;
import org.hibernate.HibernateException;
import org.hibernate.Metamodel;
import org.hibernate.Session;
import org.hibernate.SessionBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.StatelessSessionBuilder;
import org.hibernate.TypeHelper;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.spi.FilterDefinition;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metadata.CollectionMetadata;
import org.hibernate.stat.Statistics;
import org.junit.Test;

/**
 * Test for Cache jdbc blob store factory.
 */
public class CacheHibernateStoreFactorySelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** */
    static final String MODULE_PATH = "modules/hibernate-5.3/";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignite1 = startGrid(0)) {
            IgniteCache<Integer, String> cache1 = ignite1.getOrCreateCache(cacheConfiguration());

            checkStore(cache1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testXmlConfiguration() throws Exception {
        try (Ignite ignite = Ignition.start(MODULE_PATH + "/src/test/config/factory-cache.xml")) {
            try(Ignite ignite1 = Ignition.start(MODULE_PATH + "/src/test/config/factory-cache1.xml")) {
                checkStore(ignite.<Integer, String>cache(CACHE_NAME), DummySessionFactoryExt.class);

                checkStore(ignite1.<Integer, String>cache(CACHE_NAME), DummySessionFactory.class);
            }
        }
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectBeanConfiguration() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                String path = MODULE_PATH + "/src/test/config/factory-incorrect-store-cache.xml";
                try (Ignite ignite = Ignition.start(path)) {
                    ignite.cache(CACHE_NAME).getConfiguration(CacheConfiguration.class).getCacheStoreFactory().create();
                }
                return null;
            }
        }, IgniteException.class, "Failed to load bean in application context");
    }

    /**
     * @return Cache configuration with store.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

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
        @Override public StatelessSession openStatelessSession(Connection conn) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ClassMetadata getClassMetadata(Class entityCls) {
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

        @Override
        public Map<String, Object> getProperties() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isClosed() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Cache getCache() {
            return null;
        }

        @Override
        public PersistenceUnitUtil getPersistenceUnitUtil() {
            return null;
        }

        @Override
        public void addNamedQuery(String name, Query query) {

        }

        @Override
        public <T> T unwrap(Class<T> cls) {
            return null;
        }

        @Override
        public <T> void addNamedEntityGraph(String graphName, EntityGraph<T> entityGraph) {

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

        @Override
        public <T> List<EntityGraph<? super T>> findEntityGraphsByType(Class<T> aClass) {
            return null;
        }

        @Override
        public EntityManager createEntityManager() {
            return null;
        }

        @Override
        public EntityManager createEntityManager(Map map) {
            return null;
        }

        @Override
        public EntityManager createEntityManager(SynchronizationType synchronizationType) {
            return null;
        }

        @Override
        public EntityManager createEntityManager(SynchronizationType synchronizationType, Map map) {
            return null;
        }

        @Override
        public CriteriaBuilder getCriteriaBuilder() {
            return null;
        }

        @Override
        public Metamodel getMetamodel() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }
    }
}
