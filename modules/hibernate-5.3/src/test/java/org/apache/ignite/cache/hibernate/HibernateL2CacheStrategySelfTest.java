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

package org.apache.ignite.cache.hibernate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.persistence.Cacheable;
import javax.persistence.Id;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.core.Is;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory.REGION_CACHE_PROPERTY;
import static org.hibernate.cache.spi.RegionFactory.DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME;
import static org.hibernate.cache.spi.RegionFactory.DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME;
import static org.hibernate.cfg.AvailableSettings.USE_STRUCTURED_CACHE;
import static org.junit.Assert.assertThat;

/**
 * Tests Hibernate L2 cache configuration.
 */
@SuppressWarnings("unchecked")
public class HibernateL2CacheStrategySelfTest extends GridCommonAbstractTest {
    /** */
    private static final String ENTITY1_NAME = Entity1.class.getName();

    /** */
    private static final String ENTITY2_NAME = Entity2.class.getName();

    /** */
    private static final String ENTITY3_NAME = Entity3.class.getName();

    /** */
    private static final String ENTITY4_NAME = Entity4.class.getName();

    /** */
    private static final String CONNECTION_URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1";

    /** */
    private SessionFactory sesFactory1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (IgniteCacheProxy<?, ?> cache : ((IgniteKernal)grid(0)).caches())
            cache.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setCacheConfiguration(cacheConfiguration(ENTITY3_NAME),
            cacheConfiguration(ENTITY4_NAME),
            cacheConfiguration("cache1"),
            cacheConfiguration("cache2"),
            cacheConfiguration(DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME),
            cacheConfiguration(DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(cacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntityCacheReadWrite() throws Exception {
        for (AccessType accessType : new AccessType[]{AccessType.READ_WRITE, AccessType.NONSTRICT_READ_WRITE})
            testEntityCacheReadWrite(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testEntityCacheReadWrite(AccessType accessType) throws Exception {
        log.info("Test access type: " + accessType);

        sesFactory1 = startHibernate(accessType, getTestIgniteInstanceName(0));

        try {
            // 1 Adding.
            Session ses = sesFactory1.openSession();

            try {
                Transaction tr = ses.beginTransaction();

                ses.save(new Entity1(1, "entity-1#name-1"));
                ses.save(new Entity2(1, "entity-2#name-1"));

                tr.commit();
            }
            finally {
                ses.close();
            }

            loadEntities(sesFactory1);

            assertEquals(1, grid(0).cache("cache1").size());
            assertEquals(1, grid(0).cache("cache2").size());
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 1), Is.is("entity-1#name-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 1), Is.is("entity-2#name-1"));

            // 2. Updating and adding.
            ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity1 e1 = (Entity1)ses.load(Entity1.class, 1);

                e1.setName("entity-1#name-1#UPDATED-1");

                ses.update(e1);

                ses.save(new Entity2(2, "entity-2#name-2#ADDED"));

                tx.commit();
            }
            finally {
                ses.close();
            }

            loadEntities(sesFactory1);

            assertEquals(1, grid(0).cache("cache1").size());
            assertEquals(2, grid(0).cache("cache2").size());
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 1), Is.is("entity-1#name-1#UPDATED-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 1), Is.is("entity-2#name-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 2), Is.is("entity-2#name-2#ADDED"));

            // 3. Updating, adding, updating.
            ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity2 e2_1 = (Entity2)ses.load(Entity2.class, 1);

                e2_1.setName("entity-2#name-1#UPDATED-1");

                ses.update(e2_1);

                ses.save(new Entity1(2, "entity-1#name-2#ADDED"));

                Entity1 e1_1 = (Entity1)ses.load(Entity1.class, 1);

                e1_1.setName("entity-1#name-1#UPDATED-2");

                ses.update(e1_1);

                tx.commit();

            }
            finally {
                ses.close();
            }

            loadEntities(sesFactory1);

            assertEquals(2, grid(0).cache("cache1").size());
            assertEquals(2, grid(0).cache("cache2").size());
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 1), Is.is("entity-2#name-1#UPDATED-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 2), Is.is("entity-1#name-2#ADDED"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 1), Is.is("entity-1#name-1#UPDATED-2"));

            ses = sesFactory1.openSession();

            sesFactory1.getStatistics().logSummary();

            ses.close();
        }
        finally {
            cleanup();
        }
    }

    /**
     * @param sesFactory Session factory.
     */
    private void loadEntities(SessionFactory sesFactory) {
        Session ses = sesFactory.openSession();

        try {
            List<Entity1> list1 = ses.createCriteria(ENTITY1_NAME).list();

            for (Entity1 e1 : list1)
                assertNotNull(e1.getName());

            List<Entity2> list2 = ses.createCriteria(ENTITY2_NAME).list();

            for (Entity2 e2 : list2)
                assertNotNull(e2.getName());
        }
        finally {
            ses.close();
        }
    }

    /**
     * @param sesFactory Session Factory.
     * @param regionName Region Name.
     * @param id Id.
     * @return Entity Name.
     */
    private String getEntityNameFromRegion(SessionFactory sesFactory, String regionName, int id) {
        Session ses = sesFactory.openSession();

        try {
            for (Cache.Entry<Object, Object> entry : grid(0).cache(regionName)) {
                if (((HibernateKeyWrapper)entry.getKey()).id().equals(id))
                    return (String) ((HashMap) entry.getValue()).get("name");
            }

            return null;
        }
        finally {
            ses.close();
        }
    }

    /**
     * @param accessType Cache access typr.
     * @param igniteInstanceName Name of the grid providing caches.
     * @return Session factory.
     */
    private SessionFactory startHibernate(AccessType accessType, String igniteInstanceName) {
        StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();

        builder.applySetting("hibernate.connection.url", CONNECTION_URL);

        for (Map.Entry<String, String> e : HibernateL2CacheSelfTest.hibernateProperties(igniteInstanceName, accessType.name()).entrySet())
            builder.applySetting(e.getKey(), e.getValue());

        builder.applySetting(USE_STRUCTURED_CACHE, "true");
        builder.applySetting(REGION_CACHE_PROPERTY + ENTITY1_NAME, "cache1");
        builder.applySetting(REGION_CACHE_PROPERTY + ENTITY2_NAME, "cache2");
        builder.applySetting(REGION_CACHE_PROPERTY + DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME, DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME);
        builder.applySetting(REGION_CACHE_PROPERTY + DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME, DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME);

        MetadataSources metadataSources = new MetadataSources(builder.build());

        metadataSources.addAnnotatedClass(Entity1.class);
        metadataSources.addAnnotatedClass(Entity2.class);
        metadataSources.addAnnotatedClass(Entity3.class);
        metadataSources.addAnnotatedClass(Entity4.class);

        Metadata metadata = metadataSources.buildMetadata();

        for (PersistentClass entityBinding : metadata.getEntityBindings()) {
            if (!entityBinding.isInherited())
                ((RootClass)entityBinding).setCacheConcurrencyStrategy(accessType.getExternalName());
        }

        return metadata.buildSessionFactory();
    }

    /**
     * Test Hibernate entity1.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    @Cacheable
    public static class Entity1 {
        /** */
        private int id;

        /** */
        private String name;

        /**
         *
         */
        public Entity1() {
            // No-op.
        }

        /**
         * @param id ID.
         * @param name Name.
         */
        Entity1(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        public int getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * Test Hibernate entity2.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    @Cacheable
    public static class Entity2 {
        /** */
        private int id;

        /** */
        private String name;

        /**
         *
         */
        public Entity2() {
           // No-op.
        }

        /**
         * @param id ID.
         * @param name Name.
         */
        Entity2(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        public int getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * Test Hibernate entity3.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    @Cacheable
    public static class Entity3 {
        /** */
        private int id;

        /** */
        private String name;

        /**
         *
         */
        public Entity3() {
            // No-op.
        }

        /**
         * @param id ID.
         * @param name Name.
         */
        public Entity3(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        public int getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * Test Hibernate entity4.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    @Cacheable
    public static class Entity4 {
        /** */
        private int id;

        /** */
        private String name;

        /**
         *
         */
        public Entity4() {
            // No-op.
        }

        /**
         * @param id ID.
         * @param name Name.
         */
        public Entity4(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        public int getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     * Closes session factories and clears data from caches.
     *
     * @throws Exception If failed.
     */
    private void cleanup() throws Exception {
        if (sesFactory1 != null)
            sesFactory1.close();

        sesFactory1 = null;

        for (IgniteCacheProxy<?, ?> cache : ((IgniteKernal)grid(0)).caches())
            cache.clear();
    }
}
