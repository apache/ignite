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

import java.util.*;
import javax.cache.Cache;
import javax.persistence.Cacheable;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;
import org.hibernate.stat.SecondLevelCacheStatistics;
import org.springframework.cglib.core.EmitUtils;
import org.hamcrest.core.Is;
import static org.junit.Assert.assertThat;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
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


/**
 * Tests Hibernate L2 cache configuration.
 */
public class HibernateL2CacheConfigurationSelfTest extends GridCommonAbstractTest {
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
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(cacheName);

        cfg.setCacheMode(PARTITIONED);

        cfg.setAtomicityMode(ATOMIC);

        return cfg;
    }
    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Hibernate configuration.
     */
    protected Configuration hibernateConfiguration(String igniteInstanceName) {
        Configuration cfg = new Configuration();

        cfg.addAnnotatedClass(Entity1.class);
        cfg.addAnnotatedClass(Entity2.class);
        cfg.addAnnotatedClass(Entity3.class);
        cfg.addAnnotatedClass(Entity4.class);

        cfg.setProperty(DFLT_ACCESS_TYPE_PROPERTY, AccessType.NONSTRICT_READ_WRITE.name());

        cfg.setProperty(HBM2DDL_AUTO, "create");

        cfg.setProperty(GENERATE_STATISTICS, "true");

        cfg.setProperty(USE_SECOND_LEVEL_CACHE, "true");

        cfg.setProperty(USE_QUERY_CACHE, "true");

        cfg.setProperty(CACHE_REGION_FACTORY, HibernateRegionFactory.class.getName());

        cfg.setProperty(RELEASE_CONNECTIONS, "on_close");

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
     * Tests property {@link HibernateRegionFactory#REGION_CACHE_PROPERTY}.
     */
    public void testPerRegionCacheProperty() {
        testCacheUsage(1, 1, 0, 1, 1);
    }

    /**
     * Tests property {@link HibernateRegionFactory#DFLT_CACHE_NAME_PROPERTY}.
     */
    public void testDefaultCache() {
        dfltCache = true;

        testCacheUsage(1, 1, 2, 0, 0);
    }

    /**
     * @param expCache1 Expected size of cache with name 'cache1'.
     * @param expCache2 Expected size of cache with name 'cache2'.
     * @param expCache3 Expected size of cache with name 'cache3'.
     * @param expCacheE3 Expected size of cache with name {@link #ENTITY3_NAME}.
     * @param expCacheE4 Expected size of cache with name {@link #ENTITY4_NAME}.
     */
    @SuppressWarnings("unchecked")
    private void testCacheUsage(int expCache1, int expCache2, int expCache3, int expCacheE3, int expCacheE4) {
        SessionFactory sesFactory = startHibernate(getTestIgniteInstanceName(0));

        try {
            Session ses = sesFactory.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                ses.save(new Entity1());
                ses.save(new Entity2());
                ses.save(new Entity3());
                ses.save(new Entity4());

                tx.commit();
            }
            finally {
                ses.close();
            }

            ses = sesFactory.openSession();

            try {
                List<Entity1> list1 = ses.createCriteria(ENTITY1_NAME).list();

                assertEquals(1, list1.size());

                for (Entity1 e : list1) {
                    ses.load(ENTITY1_NAME, e.getId());
                    assertNotNull(e.getId());
                }

                List<Entity2> list2 = ses.createCriteria(ENTITY2_NAME).list();

                assertEquals(1, list2.size());

                for (Entity2 e : list2)
                    assertNotNull(e.getId());

                List<Entity3> list3 = ses.createCriteria(ENTITY3_NAME).list();

                assertEquals(1, list3.size());

                for (Entity3 e : list3)
                    assertNotNull(e.getId());

                List<Entity4> list4 = ses.createCriteria(ENTITY4_NAME).list();

                assertEquals(1, list4.size());

                for (Entity4 e : list4)
                    assertNotNull(e.getId());
            }
            finally {
                ses.close();
            }

            IgniteCache<Object, Object> cache1 = grid(0).cache("cache1");
            IgniteCache<Object, Object> cache2 = grid(0).cache("cache2");
            IgniteCache<Object, Object> cache3 = grid(0).cache("cache3");
            IgniteCache<Object, Object> cacheE3 = grid(0).cache(ENTITY3_NAME);
            IgniteCache<Object, Object> cacheE4 = grid(0).cache(ENTITY4_NAME);

            assertEquals("Unexpected entries: " + toSet(cache1.iterator()), expCache1, cache1.size());
            assertEquals("Unexpected entries: " + toSet(cache2.iterator()), expCache2, cache2.size());
            assertEquals("Unexpected entries: " + toSet(cache3.iterator()), expCache3, cache3.size());
            assertEquals("Unexpected entries: " + toSet(cacheE3.iterator()), expCacheE3, cacheE3.size());
            assertEquals("Unexpected entries: " + toSet(cacheE4.iterator()), expCacheE4, cacheE4.size());
        }
        finally {
            sesFactory.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntityCacheNonStrictFails() {
        SessionFactory sessionFactory
            = startHibernate(getTestIgniteInstanceName(0));

        try {
            Session session = sessionFactory.openSession();

            try {
                Transaction transaction = session.beginTransaction();

                session.save(new Entity1(0, "name1"));
                session.save(new Entity2(0, "name2"));

                transaction.commit();

            } finally {
                session.close();
            }

            session = sessionFactory.openSession();

            try {
                List<Entity1> list1 = session
                    .createCriteria(ENTITY1_NAME).list();

                assertThat(list1.size(),Is.is(1));

                for(Entity1 e1 : list1){
                    session.load(ENTITY1_NAME, e1.getId());
                    System.out.println("/n/n======== Entity1 id is " + e1.getId());
                    assertNotNull(e1.getId());
                }

                assertEquals(1, sessionFactory.getStatistics()
                    .getSecondLevelCacheStatistics(ENTITY1_NAME).getPutCount());
                assertEquals(0, sessionFactory.getStatistics()
                    .getSecondLevelCacheStatistics(ENTITY2_NAME).getPutCount());
                assertEquals(1, grid(0).cache("cache1").size());
                assertEquals(0, grid(0).cache("cache2").size());

                List<Entity2> list2 = session
                    .createCriteria(ENTITY2_NAME).list();

                assertEquals(1, list2.size());

                for(Entity2 e2 : list2){
                    session.load(ENTITY2_NAME, e2.getId());
                    System.out.println("/n/n====== Entity2 id is " + e2.getId());
                    assertNotNull(e2.getId());
                }

                assertEquals(1, sessionFactory.getStatistics()
                    .getSecondLevelCacheStatistics(ENTITY1_NAME).getPutCount());
                assertEquals(1, sessionFactory.getStatistics()
                    .getSecondLevelCacheStatistics(ENTITY2_NAME).getPutCount());
                assertEquals(1, grid(0).cache("cache1").size());
                assertEquals(1, grid(0).cache("cache2").size());

            } finally {
                session.close();
            }


            // Updtaing
            session = sessionFactory.openSession();

            session.createCriteria(ENTITY1_NAME).list();

            try {
                Transaction tx = session.beginTransaction();

                Entity1 e1 = (Entity1) session.load(Entity1.class, 0);

                session.update(e1);

                tx.commit();

            } finally {
                session.close();
            }

            session = sessionFactory.openSession();

            assertEquals(2, sessionFactory.getStatistics()
                .getSecondLevelCacheStatistics(ENTITY1_NAME).getPutCount());
            assertEquals(1, sessionFactory.getStatistics()
                .getSecondLevelCacheStatistics(ENTITY2_NAME).getPutCount());
            assertEquals(1, grid(0).cache("cache1").size());
            assertEquals(1, grid(0).cache("cache2").size());

            session = sessionFactory.openSession();

            session.createCriteria(ENTITY2_NAME).list();

            try {
                Transaction tx = session.beginTransaction();

                Entity2 e2 = (Entity2) session.load(Entity2.class, 1);

                e2.setName("name-1-changed");

                session.update(e2);

                Entity2 e3 = (Entity2) session.load(Entity2.class, 1);

                session.update(e3);

                e3.setName("name-1-changed");

                session.update(e3);

                tx.commit();

            } finally {
                session.close();
            }

            session = sessionFactory.openSession();

            assertEquals(2, sessionFactory.getStatistics()
                .getSecondLevelCacheStatistics(ENTITY1_NAME).getPutCount());
            assertEquals(3, sessionFactory.getStatistics()
                .getSecondLevelCacheStatistics(ENTITY2_NAME).getPutCount());
            assertEquals(1, grid(0).cache("cache1").size());
            assertEquals(1, grid(0).cache("cache2").size());

            printStats(sessionFactory);

            sessionFactory.getStatistics().logSummary();

            session.close();

        } finally {
            sessionFactory.close();
        }

    }

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
    private <K, V> Set<Cache.Entry<K, V>> toSet(Iterator<Cache.Entry<K, V>> iter){
        Set<Cache.Entry<K, V>> set = new HashSet<>();

        while (iter.hasNext())
            set.add(iter.next());

        return set;
    }

    /**
     * @param igniteInstanceName Name of the grid providing caches.
     * @return Session factory.
     */
    private SessionFactory startHibernate(String igniteInstanceName) {
        Configuration cfg = hibernateConfiguration(igniteInstanceName);

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
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity1 {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        public Entity1() {}

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
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity2 {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        public Entity2() {}

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
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity3 {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        public Entity3() {}

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
    }

    /**
     * Test Hibernate entity4.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    @Cacheable
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity4 {
        /** */
        private int id;
        /** */
        private String name;

        /** */
        public Entity4() {}

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
    }
}