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
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.NaturalIdCache;
import org.hibernate.cache.spi.CacheKey;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;
import org.hibernate.stat.SecondLevelCacheStatistics;

import javax.cache.Cache;
import javax.persistence.Cacheable;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.hibernate.HibernateRegionFactory.DFLT_ACCESS_TYPE_PROPERTY;
import static org.apache.ignite.cache.hibernate.HibernateRegionFactory.DFLT_CACHE_NAME_PROPERTY;
import static org.apache.ignite.cache.hibernate.HibernateRegionFactory.IGNITE_INSTANCE_NAME_PROPERTY;
import static org.apache.ignite.cache.hibernate.HibernateRegionFactory.REGION_CACHE_PROPERTY;
import static org.hibernate.cfg.AvailableSettings.CACHE_REGION_FACTORY;
import static org.hibernate.cfg.AvailableSettings.GENERATE_STATISTICS;
import static org.hibernate.cfg.AvailableSettings.HBM2DDL_AUTO;
import static org.hibernate.cfg.AvailableSettings.RELEASE_CONNECTIONS;
import static org.hibernate.cfg.AvailableSettings.USE_QUERY_CACHE;
import static org.hibernate.cfg.AvailableSettings.USE_SECOND_LEVEL_CACHE;
import static org.hibernate.cfg.AvailableSettings.USE_STRUCTURED_CACHE;
import static org.junit.Assert.assertThat;

/**
 * Tests Hibernate L2 cache configuration.
 */
public class HibernateL2CacheStrategySelfTest extends GridCommonAbstractTest {
    /** Entity names for stats output */
    private static final List<String> ENTITY_NAMES =
        Arrays.asList(Entity1.class.getName(), Entity2.class.getName());

    /** */
    public static final String ENTITY1_NAME = Entity1.class.getName();

    /** */
    public static final String ENTITY2_NAME = Entity2.class.getName();

    /** */
    public static final String ENTITY3_NAME = Entity3.class.getName();

    /** */
    public static final String ENTITY4_NAME = Entity4.class.getName();

    /** */
    public static final String TIMESTAMP_CACHE = "org.hibernate.cache.spi.UpdateTimestampsCache";

    /** */
    public static final String QUERY_CACHE = "org.hibernate.cache.internal.StandardQueryCache";

    /** */
    public static final String CONNECTION_URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1";

    /** */
    private SessionFactory sesFactory1;

    /** If {@code true} then sets default cache in configuration. */
    private boolean dfltCache;

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

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration(ENTITY3_NAME), cacheConfiguration(ENTITY4_NAME),
            cacheConfiguration("cache1"), cacheConfiguration("cache2"), cacheConfiguration("cache3"),
            cacheConfiguration(TIMESTAMP_CACHE), cacheConfiguration(QUERY_CACHE));

        return cfg;
    }

    /**
     * @return Hibernate L2 cache access types to test.
     */
    protected AccessType[] accessTypes() {
        return new AccessType[] {AccessType.READ_WRITE, AccessType.NONSTRICT_READ_WRITE};
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
     * @param accessType
     * @param igniteInstanceName Ignite instance name.
     * @return Hibernate configuration.
     */
    protected Configuration hibernateConfiguration(AccessType accessType, String igniteInstanceName) {
        Configuration cfg = new Configuration();

        cfg.addAnnotatedClass(Entity1.class);
        cfg.addAnnotatedClass(Entity2.class);
        cfg.addAnnotatedClass(Entity3.class);
        cfg.addAnnotatedClass(Entity4.class);

        cfg.setCacheConcurrencyStrategy(ENTITY1_NAME, accessType.getExternalName());
        cfg.setCacheConcurrencyStrategy(ENTITY2_NAME, accessType.getExternalName());
        cfg.setCacheConcurrencyStrategy(ENTITY3_NAME, accessType.getExternalName());
        cfg.setCacheConcurrencyStrategy(ENTITY4_NAME, accessType.getExternalName());

        cfg.setProperty(DFLT_ACCESS_TYPE_PROPERTY, accessType.name());

        cfg.setProperty(HBM2DDL_AUTO, "create");

        cfg.setProperty(GENERATE_STATISTICS, "true");

        cfg.setProperty(USE_SECOND_LEVEL_CACHE, "true");

        cfg.setProperty(USE_QUERY_CACHE, "true");

        cfg.setProperty(CACHE_REGION_FACTORY, HibernateRegionFactory.class.getName());

        cfg.setProperty(RELEASE_CONNECTIONS, "on_close");

        cfg.setProperty(USE_STRUCTURED_CACHE, "true");

        cfg.setProperty(IGNITE_INSTANCE_NAME_PROPERTY, igniteInstanceName);

        cfg.setProperty(REGION_CACHE_PROPERTY + ENTITY1_NAME, "cache1");
        cfg.setProperty(REGION_CACHE_PROPERTY + ENTITY2_NAME, "cache2");
        cfg.setProperty(REGION_CACHE_PROPERTY + TIMESTAMP_CACHE, TIMESTAMP_CACHE);
        cfg.setProperty(REGION_CACHE_PROPERTY + QUERY_CACHE, QUERY_CACHE);

        if (dfltCache)
            cfg.setProperty(DFLT_CACHE_NAME_PROPERTY, "cache3");

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntityCacheReadWrite() throws Exception {

        for (AccessType accessType : accessTypes()) {
            testEntityCacheReadWrite(accessType);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntityCacheReadWrite(AccessType accessType) throws Exception {
        sesFactory1 = startHibernate(accessType, getTestIgniteInstanceName(0));

        try {
            // I Adding
            Session session = sesFactory1.openSession();

            try {
                Transaction tr = session.beginTransaction();

                session.save(new Entity1(1, "entity-1#name-1"));
                session.save(new Entity2(1, "entity-2#name-1"));

                tr.commit();

            }
            finally {
                session.close();
            }

            printEntities(sesFactory1);

            assertThat(grid(0).cache("cache1").size(), Is.is(1));
            assertThat(grid(0).cache("cache2").size(), Is.is(1));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 1), Is.is("entity-1#name-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 1), Is.is("entity-2#name-1"));

            // II Updtaing and adding
            session = sesFactory1.openSession();

            try {
                Transaction tx = session.beginTransaction();

                Entity1 e1 = (Entity1)session.load(Entity1.class, 1);

                e1.setName("entity-1#name-1#UPDATED-1");

                session.update(e1);

                session.save(new Entity2(2, "entity-2#name-2#ADDED"));

                tx.commit();

            }
            finally {
                session.close();
            }

            printEntities(sesFactory1);

            assertThat(grid(0).cache("cache1").size(), Is.is(1));
            assertThat(grid(0).cache("cache2").size(), Is.is(2));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 1), Is.is("entity-1#name-1#UPDATED-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 1), Is.is("entity-2#name-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 2), Is.is("entity-2#name-2#ADDED"));

            // III Updating, adding, updating
            session = sesFactory1.openSession();

            try {
                Transaction tx = session.beginTransaction();

                Entity2 e2_1 = (Entity2)session.load(Entity2.class, 1);

                e2_1.setName("entity-2#name-1#UPDATED-1");

                session.update(e2_1);

                session.save(new Entity1(2, "entity-1#name-2#ADDED"));

                Entity1 e1_1 = (Entity1)session.load(Entity1.class, 1);

                e1_1.setName("entity-1#name-1#UPDATED-2");

                session.update(e1_1);

                tx.commit();

            }
            finally {
                session.close();
            }

            printEntities(sesFactory1);

            assertThat(grid(0).cache("cache1").size(), Is.is(2));
            assertThat(grid(0).cache("cache2").size(), Is.is(2));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache2", 1), Is.is("entity-2#name-1#UPDATED-1"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 2), Is.is("entity-1#name-2#ADDED"));
            assertThat(getEntityNameFromRegion(sesFactory1, "cache1", 1), Is.is("entity-1#name-1#UPDATED-2"));

            session = sesFactory1.openSession();

            printStats(sesFactory1);

            sesFactory1.getStatistics().logSummary();

            session.close();

        }
        finally {
            cleanup();
        }
    }

    /**
     *
     */
    private void printEntities(SessionFactory sessionFactory) {
        Session ses = sessionFactory.openSession();

        List<Entity1> list1 = ses
            .createCriteria(ENTITY1_NAME).list();

        for (Entity1 e1 : list1) {
            ses.load(ENTITY1_NAME, e1.getId());
            System.out.println("/n/n ======== Entity1: id is " + e1.getId() + " name is " + e1.getName());
            assertNotNull(e1.getId());
        }

        List<Entity2> list2 = ses
            .createCriteria(ENTITY2_NAME).list();

        for (Entity2 e2 : list2) {
            ses.load(ENTITY2_NAME, e2.getId());
            System.out.println("/n/n ======== Entity2: id is " + e2.getId() + " name is " + e2.getName());
            assertNotNull(e2.getId());
        }
    }

    /**
     * @param sesFactory Session Factory.
     * @param regionName Region Name.
     * @param id Id.
     * @return Entity Name.
     */
    private String getEntityNameFromRegion(SessionFactory sesFactory, String regionName, int id) {
        String entityName = null;

        Session ses = sesFactory.openSession();

        try {
            Iterator<Cache.Entry<Object, Object>> iter = grid(0).cache(regionName).iterator();

            while (iter.hasNext()) {
                Cache.Entry<Object, Object> entry = iter.next();
                if (((CacheKey)entry.getKey()).getKey().equals(id)) {
                    entityName = (String)((HashMap)entry.getValue()).get("name");
                    break;
                }
            }

            return entityName;

        }
        finally {
            ses.close();
        }
    }

    /**
     * @param sessionFactory Session Factory.
     */
    private void printStats(SessionFactory sessionFactory) {
        System.out.println("===  Hibernate L2 cache statistics  ====");

        for (String entityName : ENTITY_NAMES) {
            System.out.println("\tEntity: " + entityName);

            SecondLevelCacheStatistics statistics =
                sessionFactory.getStatistics().getSecondLevelCacheStatistics(entityName);

            System.out.println("\t\tPuts: " + statistics.getPutCount());
            System.out.println("\t\tHits: " + statistics.getHitCount());
            System.out.println("\t\tMisses: " + statistics.getMissCount());
        }

        System.out.println("========================================");
    }

    /**
     *
     */
    private <K, V> Set<Cache.Entry<K, V>> toSet(Iterator<Cache.Entry<K, V>> iter) {
        Set<Cache.Entry<K, V>> set = new HashSet<>();

        while (iter.hasNext())
            set.add(iter.next());

        return set;
    }

    /**
     * @param accessType
     * @param igniteInstanceName Name of the grid providing caches.
     * @return Session factory.
     */
    private SessionFactory startHibernate(AccessType accessType, String igniteInstanceName) {
        Configuration cfg = hibernateConfiguration(accessType, igniteInstanceName);

        ServiceRegistryBuilder builder = new ServiceRegistryBuilder();

        builder.applySetting("hibernate.connection.url", CONNECTION_URL);
        builder.applySetting("hibernate.show_sql", false);

        return cfg.buildSessionFactory(builder.buildServiceRegistry());
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

        /** */
        public Entity1() {
        }

        /** */
        public Entity1(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        @GeneratedValue
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

        /** */
        public Entity2() {
        }

        /** */
        public Entity2(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        @GeneratedValue
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

        /** */
        public Entity3() {
        }

        /** */
        public Entity3(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        @GeneratedValue
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

        /** */
        public Entity4() {
        }

        /** */
        public Entity4(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        @Id
        @GeneratedValue
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