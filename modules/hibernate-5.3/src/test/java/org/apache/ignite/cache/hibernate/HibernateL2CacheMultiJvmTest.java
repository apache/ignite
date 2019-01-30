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

import java.util.Map;
import javax.persistence.Cacheable;
import javax.persistence.Id;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.hibernate.HibernateL2CacheSelfTest.CONNECTION_URL;
import static org.apache.ignite.cache.hibernate.HibernateL2CacheSelfTest.hibernateProperties;
import static org.hibernate.cache.spi.RegionFactory.DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME;
import static org.hibernate.cache.spi.access.AccessType.NONSTRICT_READ_WRITE;

/**
 *
 */
public class HibernateL2CacheMultiJvmTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(
            cacheConfiguration(DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME),
            cacheConfiguration(Entity1.class.getName()),
            cacheConfiguration(Entity2.class.getName()),
            cacheConfiguration(Entity3.class.getName())
        );

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setName(cacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testL2Cache() throws Exception {
        Ignite srv = ignite(0);

        {
            IgniteCompute client1Compute =
                srv.compute(srv.cluster().forNodeId(ignite(1).cluster().localNode().id()));

            client1Compute.run(new HibernateInsertRunnable());
        }

        {
            IgniteCompute client2Compute =
                srv.compute(srv.cluster().forNodeId(ignite(2).cluster().localNode().id()));

            client2Compute.run(new HibernateLoadRunnable());
        }

        {
            IgniteCompute srvCompute = srv.compute(srv.cluster().forLocal());

            srvCompute.run(new HibernateLoadRunnable());
        }
    }

    /**
     *
     */
    private static class HibernateInsertRunnable extends HibernateBaseRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            SessionFactory sesFactory = startHibernate(ignite.name());

            Session ses = sesFactory.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 1; i++) {
                    {
                        Entity1 e = new Entity1();
                        e.setId(i);
                        e.setName("name-" + i);

                        ses.save(e);
                    }

                    {
                        Entity2 e = new Entity2();
                        e.setId(String.valueOf(i));
                        e.setName("name-" + i);

                        ses.save(e);
                    }

                    {
                        Entity3 e = new Entity3();
                        e.setId(i);
                        e.setName("name-" + i);

                        ses.save(e);
                    }
                }

                tx.commit();
            }
            finally {
                ses.close();
            }
        }
    }

    /**
     *
     */
    private static class HibernateLoadRunnable extends HibernateBaseRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            SessionFactory sesFactory = startHibernate(ignite.name());

            Session ses = sesFactory.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 1; i++) {
                    {
                        Entity1 e = (Entity1)ses.load(Entity1.class, i);

                        log.info("Found: " + e.getName());
                    }
                    {
                        Entity2 e = (Entity2)ses.load(Entity2.class, String.valueOf(i));

                        log.info("Found: " + e.getName());
                    }
                    {
                        Entity3 e = (Entity3)ses.load(Entity3.class, (double)i);

                        log.info("Found: " + e.getName());
                    }
                }

                tx.commit();
            }
            finally {
                ses.close();
            }
        }
    }

    /**
     *
     */
    private abstract static class HibernateBaseRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** */
        @LoggerResource
        IgniteLogger log;

        /**
         * @param nodeName Name of the grid providing caches.
         * @return Session factory.
         */
        SessionFactory startHibernate(String nodeName) {
            log.info("Start hibernate on node: " + nodeName);

            StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();

            for (Map.Entry<String, String> e : hibernateProperties(nodeName, NONSTRICT_READ_WRITE.name()).entrySet())
                builder.applySetting(e.getKey(), e.getValue());

            builder.applySetting("hibernate.connection.url", CONNECTION_URL);

            MetadataSources metadataSources = new MetadataSources(builder.build());

            metadataSources.addAnnotatedClass(Entity1.class);
            metadataSources.addAnnotatedClass(Entity2.class);
            metadataSources.addAnnotatedClass(Entity3.class);

            return metadataSources.buildMetadata().buildSessionFactory();
        }
    }

    /**
     * Test Hibernate entity1.
     */
    @javax.persistence.Entity
    @Cacheable
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity1 {
        /** */
        @Id
        private int id;

        /** */
        private String name;

        /**
         * @return ID.
         */
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Entity1 entity1 = (Entity1)o;

            return id == entity1.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     * Test Hibernate entity1.
     */
    @javax.persistence.Entity
    @Cacheable
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity2 {
        /** */
        @Id
        private String id;

        /** */
        private String name;

        /**
         * @return ID.
         */
        public String getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(String id) {
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Entity2 entity2 = (Entity2)o;

            return id.equals(entity2.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }
    }

    /**
     * Test Hibernate entity1.
     */
    @javax.persistence.Entity
    @Cacheable
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
    public static class Entity3 {
        /** */
        @Id
        private double id;

        /** */
        private String name;

        /**
         * @return ID.
         */
        public double getId() {
            return id;
        }

        /**
         * @param id ID.
         */
        public void setId(double id) {
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Entity3 entity3 = (Entity3)o;

            return Double.compare(entity3.id, id) == 0;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            long temp = Double.doubleToLongBits(id);
            return (int)(temp ^ (temp >>> 32));
        }
    }
}
