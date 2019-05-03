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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import javax.persistence.SharedCacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.NaturalIdCache;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;
import org.hibernate.query.Query;
import org.hibernate.stat.CacheRegionStatistics;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory.DFLT_ACCESS_TYPE_PROPERTY;
import static org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory.IGNITE_INSTANCE_NAME_PROPERTY;
import static org.apache.ignite.cache.hibernate.HibernateAccessStrategyFactory.REGION_CACHE_PROPERTY;
import static org.hibernate.cache.spi.RegionFactory.DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME;
import static org.hibernate.cache.spi.RegionFactory.DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME;
import static org.hibernate.cfg.AvailableSettings.JPA_SHARED_CACHE_MODE;
import static org.hibernate.cfg.Environment.CACHE_REGION_FACTORY;
import static org.hibernate.cfg.Environment.GENERATE_STATISTICS;
import static org.hibernate.cfg.Environment.HBM2DDL_AUTO;
import static org.hibernate.cfg.Environment.RELEASE_CONNECTIONS;
import static org.hibernate.cfg.Environment.USE_QUERY_CACHE;
import static org.hibernate.cfg.Environment.USE_SECOND_LEVEL_CACHE;

/**
 *
 * Tests Hibernate L2 cache.
 */
public class HibernateL2CacheSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final String CONNECTION_URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1";

    /** */
    public static final String ENTITY_NAME = Entity.class.getName();

    /** */
    public static final String ENTITY2_NAME = Entity2.class.getName();

    /** */
    public static final String VERSIONED_ENTITY_NAME = VersionedEntity.class.getName();

    /** */
    public static final String CHILD_ENTITY_NAME = ChildEntity.class.getName();

    /** */
    public static final String PARENT_ENTITY_NAME = ParentEntity.class.getName();

    /** */
    public static final String CHILD_COLLECTION_REGION = ENTITY_NAME + ".children";

    /** */
    public static final String NATURAL_ID_REGION =
        "org.apache.ignite.cache.hibernate.HibernateL2CacheSelfTest$Entity##NaturalId";

    /** */
    public static final String NATURAL_ID_REGION2 =
        "org.apache.ignite.cache.hibernate.HibernateL2CacheSelfTest$Entity2##NaturalId";

    /** */
    private SessionFactory sesFactory1;

    /** */
    private SessionFactory sesFactory2;

    /**
     * First Hibernate test entity.
     */
    @javax.persistence.Entity
    @NaturalIdCache
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    public static class Entity {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        private Collection<ChildEntity> children;

        /**
         * Default constructor required by Hibernate.
         */
        public Entity() {
            // No-op.
        }

        /**
         * @param id ID.
         * @param name Name.
         */
        public Entity(int id, String name) {
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
        @NaturalId(mutable = true)
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return Children.
         */
        @OneToMany(cascade=javax.persistence.CascadeType.ALL, fetch=FetchType.LAZY)
        @JoinColumn(name="ENTITY_ID")
        public Collection<ChildEntity> getChildren() {
            return children;
        }

        /**
         * @param children Children.
         */
        public void setChildren(Collection<ChildEntity> children) {
            this.children = children;
        }
    }

    /**
     * Second Hibernate test entity.
     */
    @javax.persistence.Entity
    @NaturalIdCache
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    public static class Entity2 {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        private Collection<ChildEntity> children;

        /**
         * Default constructor required by Hibernate.
         */
        public Entity2() {
            // No-op.
        }

        /**
         * @param id ID.
         * @param name Name.
         */
        public Entity2(int id, String name) {
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
        @NaturalId(mutable = true)
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
     * Hibernate child entity referenced by {@link Entity}.
     */
    @javax.persistence.Entity
    @SuppressWarnings("PublicInnerClass")
    public static class ChildEntity {
        /** */
        private int id;

        /**
         * Default constructor required by Hibernate.
         */
        public ChildEntity() {
            // No-op.
        }

        /**
         * @param id ID.
         */
        public ChildEntity(int id) {
            this.id = id;
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
     * Hibernate entity referencing {@link Entity}.
     */
    @javax.persistence.Entity
    @SuppressWarnings("PublicInnerClass")
    public static class ParentEntity {
        /** */
        private int id;

        /** */
        private Entity entity;

        /**
         * Default constructor required by Hibernate.
         */
        public ParentEntity() {
            // No-op.
        }

        /**
         * @param id ID.
         * @param entity Referenced entity.
         */
        public ParentEntity(int id, Entity entity) {
            this.id = id;
            this.entity = entity;
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
         * @return Referenced entity.
         */
        @OneToOne
        public Entity getEntity() {
            return entity;
        }

        /**
         * @param entity Referenced entity.
         */
        public void setEntity(Entity entity) {
            this.entity = entity;
        }
    }

    /**
     * Hibernate entity.
     */
    @javax.persistence.Entity
    @SuppressWarnings({"PublicInnerClass", "UnnecessaryFullyQualifiedName"})
    public static class VersionedEntity {
        /** */
        private int id;

        /** */
        private long ver;

        /**
         * Default constructor required by Hibernate.
         */
        public VersionedEntity() {
        }

        /**
         * @param id ID.
         */
        public VersionedEntity(int id) {
            this.id = id;
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
         * @return Version.
         */
        @javax.persistence.Version
        public long getVersion() {
            return ver;
        }

        /**
         * @param ver Version.
         */
        public void setVersion(long ver) {
            this.ver = ver;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(generalRegionConfiguration(DEFAULT_UPDATE_TIMESTAMPS_REGION_UNQUALIFIED_NAME),
            generalRegionConfiguration(DEFAULT_QUERY_RESULTS_REGION_UNQUALIFIED_NAME),
            transactionalRegionConfiguration(ENTITY_NAME),
            transactionalRegionConfiguration(ENTITY2_NAME),
            transactionalRegionConfiguration(VERSIONED_ENTITY_NAME),
            transactionalRegionConfiguration(PARENT_ENTITY_NAME),
            transactionalRegionConfiguration(CHILD_ENTITY_NAME),
            transactionalRegionConfiguration(CHILD_COLLECTION_REGION),
            transactionalRegionConfiguration(NATURAL_ID_REGION),
            transactionalRegionConfiguration(NATURAL_ID_REGION2));

        return cfg;
    }

    /**
     * @param regionName Region name.
     * @return Cache configuration for {@link IgniteGeneralDataRegion}.
     */
    private CacheConfiguration generalRegionConfiguration(String regionName) {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(regionName);

        cfg.setCacheMode(PARTITIONED);

        cfg.setAtomicityMode(ATOMIC);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setBackups(1);

        cfg.setStatisticsEnabled(true);

        //cfg.setAffinity(new RendezvousAffinityFunction(false, 10));

        return cfg;
    }

    /**
     * @param regionName Region name.
     * @return Transactional Cache configuration
     */
    protected CacheConfiguration transactionalRegionConfiguration(String regionName) {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(regionName);

        cfg.setCacheMode(PARTITIONED);

        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setBackups(1);

        cfg.setStatisticsEnabled(true);

        cfg.setAffinity(new RendezvousAffinityFunction(false, 10));

        return cfg;
    }

    /**
     * @return Hibernate registry builder.
     */
    protected StandardServiceRegistryBuilder registryBuilder() {
        StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();

        builder.applySetting("hibernate.connection.url", CONNECTION_URL);

        return builder;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanup();
    }

    /**
     * @return Hibernate L2 cache access types to test.
     */
    protected AccessType[] accessTypes() {
        return new AccessType[]{AccessType.READ_ONLY, AccessType.NONSTRICT_READ_WRITE, AccessType.READ_WRITE};
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCollectionCache() throws Exception {
        for (AccessType accessType : accessTypes())
            testCollectionCache(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testCollectionCache(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        Map<Integer, Integer> idToChildCnt = new HashMap<>();

        try {
            Session ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 3; i++) {
                    Entity e = new Entity(i, "name-" + i);

                    Collection<ChildEntity> children = new ArrayList<>();

                    for (int j = 0; j < 3; j++)
                        children.add(new ChildEntity());

                    e.setChildren(children);

                    idToChildCnt.put(e.getId(), e.getChildren().size());

                    ses.save(e);
                }

                tx.commit();
            }
            finally {
                ses.close();
            }

            // Load children, this should populate cache.

            ses = sesFactory1.openSession();

            try {
                List<Entity> list = ses.createCriteria(ENTITY_NAME).list();

                assertEquals(idToChildCnt.size(), list.size());

                for (Entity e : list)
                    assertEquals((int) idToChildCnt.get(e.getId()), e.getChildren().size());
            }
            finally {
                ses.close();
            }

            assertCollectionCache(sesFactory2, idToChildCnt, 3, 0);
            assertCollectionCache(sesFactory1, idToChildCnt, 3, 0);

            if (accessType == AccessType.READ_ONLY)
                return;

            // Update children for one entity.

            ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity e1 = (Entity)ses.load(Entity.class, 1);

                e1.getChildren().remove(e1.getChildren().iterator().next());

                ses.update(e1);

                idToChildCnt.put(e1.getId(), e1.getChildren().size());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertCollectionCache(sesFactory2, idToChildCnt, 2, 1); // After update collection cache entry is removed.
            assertCollectionCache(sesFactory1, idToChildCnt, 3, 0); // 'assertCollectionCache' loads children in cache.

            // Update children for the same entity using another SessionFactory.

            ses = sesFactory2.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity e1 = (Entity)ses.load(Entity.class, 1);

                e1.getChildren().remove(e1.getChildren().iterator().next());

                ses.update(e1);

                idToChildCnt.put(e1.getId(), e1.getChildren().size());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertCollectionCache(sesFactory2, idToChildCnt, 2, 1); // After update collection cache entry is removed.
            assertCollectionCache(sesFactory1, idToChildCnt, 3, 0); // 'assertCollectionCache' loads children in cache.
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntityCache() throws Exception {
        for (AccessType accessType : accessTypes())
            testEntityCache(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testEntityCache(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        Map<Integer, String> idToName = new HashMap<>();

        try {
            Session ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 2; i++) {
                    String name = "name-" + i;

                    ses.save(new Entity(i, name));

                    idToName.put(i, name);
                }

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName, 100);

            if (accessType == AccessType.READ_ONLY)
                return;

            ses = sesFactory1.openSession();

            try {
                // Updates and inserts in single transaction.

                Transaction tx = ses.beginTransaction();

                Entity e0 = (Entity)ses.load(Entity.class, 0);

                e0.setName("name-0-changed1");

                ses.update(e0);

                idToName.put(0, e0.getName());

                ses.save(new Entity(2, "name-2"));

                idToName.put(2, "name-2");

                Entity e1 = (Entity)ses.load(Entity.class, 1);

                e1.setName("name-1-changed1");

                ses.update(e1);

                idToName.put(1, e1.getName());

                ses.save(new Entity(3, "name-3"));

                idToName.put(3, "name-3");

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName);

            ses = sesFactory1.openSession();

            try {
                // Updates, inserts and deletes in single transaction.

                Transaction tx = ses.beginTransaction();

                ses.save(new Entity(4, "name-4"));

                idToName.put(4, "name-4");

                Entity e0 = (Entity)ses.load(Entity.class, 0);

                e0.setName("name-0-changed2");

                ses.update(e0);

                idToName.put(e0.getId(), e0.getName());

                ses.delete(ses.load(Entity.class, 1));

                idToName.remove(1);

                Entity e2 = (Entity)ses.load(Entity.class, 2);

                e2.setName("name-2-changed1");

                ses.update(e2);

                idToName.put(e2.getId(), e2.getName());

                ses.delete(ses.load(Entity.class, 3));

                idToName.remove(3);

                ses.save(new Entity(5, "name-5"));

                idToName.put(5, "name-5");

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName, 1, 3);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName, 1, 3);

            // Try to update the same entity using another SessionFactory.

            ses = sesFactory2.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity e0 = (Entity)ses.load(Entity.class, 0);

                e0.setName("name-0-changed3");

                ses.update(e0);

                idToName.put(e0.getId(), e0.getName());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName);
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTwoEntitiesSameCache() throws Exception {
        for (AccessType accessType : accessTypes())
            testTwoEntitiesSameCache(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testTwoEntitiesSameCache(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        try {
            Session ses = sesFactory1.openSession();

            Map<Integer, String> idToName1 = new HashMap<>();
            Map<Integer, String> idToName2 = new HashMap<>();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 2; i++) {
                    String name = "name-" + i;

                    ses.save(new Entity(i, name));
                    ses.save(new Entity2(i, name));

                    idToName1.put(i, name);
                    idToName2.put(i, name);
                }

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName1, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName1, 100);

            assertEntityCache(ENTITY2_NAME, sesFactory2, idToName2, 100);
            assertEntityCache(ENTITY2_NAME, sesFactory1, idToName2, 100);

            if (accessType == AccessType.READ_ONLY)
                return;

            ses = sesFactory1.openSession();

            try {
                // Updates both entities in single transaction.

                Transaction tx = ses.beginTransaction();

                Entity e = (Entity)ses.load(Entity.class, 0);

                e.setName("name-0-changed1");

                ses.update(e);

                Entity2 e2 = (Entity2)ses.load(Entity2.class, 0);

                e2.setName("name-e2-0-changed1");

                ses.update(e2);

                idToName1.put(0, e.getName());
                idToName2.put(0, e2.getName());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName1, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName1, 100);

            assertEntityCache(ENTITY2_NAME, sesFactory2, idToName2, 100);
            assertEntityCache(ENTITY2_NAME, sesFactory1, idToName2, 100);

            ses = sesFactory1.openSession();

            try {
                // Remove entity1 and insert entity2 in single transaction.

                Transaction tx = ses.beginTransaction();

                Entity e = (Entity)ses.load(Entity.class, 0);

                ses.delete(e);

                Entity2 e2 = new Entity2(2, "name-2");

                ses.save(e2);

                idToName1.remove(0);
                idToName2.put(2, e2.getName());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName1, 0, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName1, 0, 100);

            assertEntityCache(ENTITY2_NAME, sesFactory2, idToName2, 100);
            assertEntityCache(ENTITY2_NAME, sesFactory1, idToName2, 100);

            ses = sesFactory1.openSession();

            Transaction tx = ses.beginTransaction();

            try {
                // Update, remove, insert in single transaction, transaction fails.

                Entity e = (Entity)ses.load(Entity.class, 1);

                e.setName("name-1-changed1");

                ses.update(e); // Valid update.

                ses.save(new Entity(2, "name-2")); // Valid insert.

                ses.delete(ses.load(Entity2.class, 0)); // Valid delete.

                Entity2 e2 = (Entity2)ses.load(Entity2.class, 1);

                e2.setName("name-2");  // Invalid update, not-unique name.

                ses.update(e2);

                tx.commit();

                fail("Commit must fail.");
            }
            catch (PersistenceException e) {
                assertEquals(ConstraintViolationException.class, e.getCause().getClass());
                log.info("Expected exception: " + e);

                tx.rollback();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName1, 0, 2, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName1, 0, 2, 100);

            assertEntityCache(ENTITY2_NAME, sesFactory2, idToName2, 100);
            assertEntityCache(ENTITY2_NAME, sesFactory1, idToName2, 100);

            ses = sesFactory2.openSession();

            try {
                // Update, remove, insert in single transaction.

                tx = ses.beginTransaction();

                Entity e = (Entity)ses.load(Entity.class, 1);

                e.setName("name-1-changed1");

                ses.update(e);

                idToName1.put(1, e.getName());

                ses.save(new Entity(2, "name-2"));

                idToName1.put(2, "name-2");

                ses.delete(ses.load(Entity2.class, 0));

                idToName2.remove(0);

                Entity2 e2 = (Entity2)ses.load(Entity2.class, 1);

                e2.setName("name-e2-2-changed");

                ses.update(e2);

                idToName2.put(1, e2.getName());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName1, 0, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName1, 0, 100);

            assertEntityCache(ENTITY2_NAME, sesFactory2, idToName2, 0, 100);
            assertEntityCache(ENTITY2_NAME, sesFactory1, idToName2, 0, 100);
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntity() throws Exception {
        for (AccessType accessType : accessTypes())
            testVersionedEntity(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testVersionedEntity(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        try {
            Session ses = sesFactory1.openSession();

            VersionedEntity e0 = new VersionedEntity(0);

            try {
                Transaction tx = ses.beginTransaction();

                ses.save(e0);

                tx.commit();
            }
            finally {
                ses.close();
            }

            ses = sesFactory1.openSession();

            long ver;

            try {
                ver = ((VersionedEntity)ses.load(VersionedEntity.class, 0)).getVersion();
            }
            finally {
                ses.close();
            }

            CacheRegionStatistics stats1 =
                sesFactory1.getStatistics().getCacheRegionStatistics(VERSIONED_ENTITY_NAME);
            CacheRegionStatistics stats2 =
                sesFactory2.getStatistics().getCacheRegionStatistics(VERSIONED_ENTITY_NAME);

            assertEquals(1, stats1.getElementCountInMemory());
            assertEquals(1, stats2.getElementCountInMemory());

            ses = sesFactory2.openSession();

            try {
                assertEquals(ver, ((VersionedEntity)ses.load(VersionedEntity.class, 0)).getVersion());
            }
            finally {
                ses.close();
            }

            assertEquals(1, stats2.getElementCountInMemory());
            assertEquals(1, stats2.getHitCount());

            if (accessType == AccessType.READ_ONLY)
                return;

            e0.setVersion(ver - 1);

            ses = sesFactory1.openSession();

            Transaction tx = ses.beginTransaction();

            try {
                ses.update(e0);

                tx.commit();

                fail("Commit must fail.");
            }
            catch (OptimisticLockException e) {
                log.info("Expected exception: " + e);
            }
            finally {
                tx.rollback();

                ses.close();
            }

            sesFactory1.getStatistics().clear();

            stats1 = sesFactory1.getStatistics().getCacheRegionStatistics(VERSIONED_ENTITY_NAME);

            ses = sesFactory1.openSession();

            try {
                assertEquals(ver, ((VersionedEntity)ses.load(VersionedEntity.class, 0)).getVersion());
            }
            finally {
                ses.close();
            }

            assertEquals(1, stats1.getElementCountInMemory());
            assertEquals(1, stats1.getHitCount());
            assertEquals(1, stats2.getElementCountInMemory());
            assertEquals(1, stats2.getHitCount());
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNaturalIdCache() throws Exception {
        for (AccessType accessType : accessTypes())
            testNaturalIdCache(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testNaturalIdCache(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        Map<String, Integer> nameToId = new HashMap<>();

        try {
            Session ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 3; i++) {
                    String name = "name-" + i;

                    ses.save(new Entity(i, name));

                    nameToId.put(name, i);
                }

                tx.commit();
            }
            finally {
                ses.close();
            }

            ses = sesFactory1.openSession();

            try {
                for (Map.Entry<String, Integer> e : nameToId.entrySet())
                    ((Entity) ses.bySimpleNaturalId(Entity.class).load(e.getKey())).getId();
            }
            finally {
                ses.close();
            }

            assertNaturalIdCache(sesFactory2, nameToId, "name-100");
            assertNaturalIdCache(sesFactory1, nameToId, "name-100");

            if (accessType == AccessType.READ_ONLY)
                return;

            // Update naturalId.

            ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity e1 = (Entity)ses.load(Entity.class, 1);

                nameToId.remove(e1.getName());

                e1.setName("name-1-changed1");

                nameToId.put(e1.getName(), e1.getId());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertNaturalIdCache(sesFactory2, nameToId, "name-1");
            assertNaturalIdCache(sesFactory1, nameToId, "name-1");

            // Update entity using another SessionFactory.

            ses = sesFactory2.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Entity e1 = (Entity)ses.load(Entity.class, 1);

                nameToId.remove(e1.getName());

                e1.setName("name-1-changed2");

                nameToId.put(e1.getName(), e1.getId());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertNaturalIdCache(sesFactory2, nameToId, "name-1-changed1");
            assertNaturalIdCache(sesFactory1, nameToId, "name-1-changed1");

            // Try invalid NaturalId update.

            ses = sesFactory1.openSession();

            Transaction tx = ses.beginTransaction();

            try {
                Entity e1 = (Entity)ses.load(Entity.class, 1);

                e1.setName("name-0"); // Invalid update (duplicated name).

                tx.commit();

                fail("Commit must fail.");
            }
            catch (PersistenceException e) {
                assertEquals(ConstraintViolationException.class, e.getCause().getClass());
                log.info("Expected exception: " + e);

                tx.rollback();
            }
            finally {
                ses.close();
            }

            assertNaturalIdCache(sesFactory2, nameToId);
            assertNaturalIdCache(sesFactory1, nameToId);

            // Delete entity.

            ses = sesFactory2.openSession();

            try {
                tx = ses.beginTransaction();

                Entity e2 = (Entity)ses.load(Entity.class, 2);

                ses.delete(e2);

                nameToId.remove(e2.getName());

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertNaturalIdCache(sesFactory2, nameToId, "name-2");
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntityCacheTransactionFails() throws Exception {
        for (AccessType accessType : accessTypes())
            testEntityCacheTransactionFails(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testEntityCacheTransactionFails(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        Map<Integer, String> idToName = new HashMap<>();

        try {
            Session ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 3; i++) {
                    String name = "name-" + i;

                    ses.save(new Entity(i, name));

                    idToName.put(i, name);
                }

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName, 100);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName, 100);

            ses = sesFactory1.openSession();

            Transaction tx = ses.beginTransaction();

            try {
                ses.save(new Entity(3, "name-3")); // Valid insert.

                ses.save(new Entity(0, "name-0")); // Invalid insert (duplicated ID).

                tx.commit();

                fail("Commit must fail.");
            }
            catch (PersistenceException e) {
                assertEquals(ConstraintViolationException.class, e.getCause().getClass());
                log.info("Expected exception: " + e);

                tx.rollback();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName, 3);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName, 3);

            if (accessType == AccessType.READ_ONLY)
                return;

            ses = sesFactory1.openSession();

            tx = ses.beginTransaction();

            try {
                Entity e0 = (Entity)ses.load(Entity.class, 0);
                Entity e1 = (Entity)ses.load(Entity.class, 1);

                e0.setName("name-10"); // Valid update.
                e1.setName("name-2"); // Invalid update (violates unique constraint).

                ses.update(e0);
                ses.update(e1);

                tx.commit();

                fail("Commit must fail.");
            }
            catch (PersistenceException e) {
                assertEquals(ConstraintViolationException.class, e.getCause().getClass());
                log.info("Expected exception: " + e);

                tx.rollback();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName);

            ses = sesFactory1.openSession();

            try {
                // Create parent entity referencing Entity with ID = 0.

                tx = ses.beginTransaction();

                ses.save(new ParentEntity(0, (Entity) ses.load(Entity.class, 0)));

                tx.commit();
            }
            finally {
                ses.close();
            }

            ses = sesFactory1.openSession();

            tx = ses.beginTransaction();

            try {
                ses.save(new Entity(3, "name-3")); // Valid insert.

                Entity e1 = (Entity)ses.load(Entity.class, 1);

                e1.setName("name-10"); // Valid update.

                ses.delete(ses.load(Entity.class, 0)); // Invalid delete (there is a parent entity referencing it).

                tx.commit();

                fail("Commit must fail.");
            }
            catch (PersistenceException e) {
                assertEquals(ConstraintViolationException.class, e.getCause().getClass());
                log.info("Expected exception: " + e);

                tx.rollback();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName, 3);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName, 3);

            ses = sesFactory1.openSession();

            tx = ses.beginTransaction();

            try {
                ses.delete(ses.load(Entity.class, 1)); // Valid delete.

                idToName.remove(1);

                ses.delete(ses.load(Entity.class, 0)); // Invalid delete (there is a parent entity referencing it).

                tx.commit();

                fail("Commit must fail.");
            }
            catch (PersistenceException e) {
                assertEquals(ConstraintViolationException.class, e.getCause().getClass());
                log.info("Expected exception: " + e);

                tx.rollback();
            }
            finally {
                ses.close();
            }

            assertEntityCache(ENTITY_NAME, sesFactory2, idToName);
            assertEntityCache(ENTITY_NAME, sesFactory1, idToName);
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryCache() throws Exception {
        for (AccessType accessType : accessTypes())
            testQueryCache(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testQueryCache(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        try {
            Session ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < 5; i++)
                    ses.save(new Entity(i, "name-" + i));

                tx.commit();
            }
            finally {
                ses.close();
            }

            // This sleep is needed in the case that the transaction above completes and the next transaction
            // starts in less than 1 ms, meaning the transaction timestamps are the same.
            // In this scenario the entity update time == query cache update time since the times are stored
            // using the transaction start time.  If those values are equal then the query cache will not be
            // used in the third transaction and the failure will manifest itself in this assertion
            // 'assertEquals(2, sesFactory2.getStatistics().getQueryCacheHitCount());'
            // This behavior is in TimestampsCacheEnabledImpl.isUpToDate() line: if ( lastUpdate >= timestamp ) {...}
            // It is not a hibernate bug, ms just doesn't have the best time resolution.
            // The way to fix it may be to update the HibernateRegionFactory.nextTimestamp()
            // to return a microsecond timestamp.  This may be doable in java 9.
            Thread.sleep(1);

            // Run some queries.

            ses = sesFactory1.openSession();

            try {
                Query qry1 = ses.createQuery("from " + ENTITY_NAME + " where id > 2");

                qry1.setCacheable(true);

                assertEquals(2, qry1.list().size());

                Query qry2 = ses.createQuery("from " + ENTITY_NAME + " where name = 'name-0'");

                qry2.setCacheable(true);

                assertEquals(1, qry2.list().size());
            }
            finally {
                ses.close();
            }

            assertEquals(0, sesFactory1.getStatistics().getQueryCacheHitCount());
            assertEquals(2, sesFactory1.getStatistics().getQueryCacheMissCount());
            assertEquals(2, sesFactory1.getStatistics().getQueryCachePutCount());

            // Run queries using another SessionFactory.  The first two queries should now be cached

            ses = sesFactory2.openSession();

            try {
                Query qry1 = ses.createQuery("from " + ENTITY_NAME + " where id > 2");

                qry1.setCacheable(true);

                assertEquals(2, qry1.list().size());

                Query qry2 = ses.createQuery("from " + ENTITY_NAME + " where name = 'name-0'");

                qry2.setCacheable(true);

                assertEquals(1, qry2.list().size());

                Query qry3 = ses.createQuery("from " + ENTITY_NAME + " where id > 1");

                qry3.setCacheable(true);

                assertEquals(3, qry3.list().size());
            }
            finally {
                ses.close();
            }

            assertEquals(2, sesFactory2.getStatistics().getQueryCacheHitCount());
            assertEquals(1, sesFactory2.getStatistics().getQueryCacheMissCount());
            assertEquals(1, sesFactory2.getStatistics().getQueryCachePutCount());

            // Update entity, it should invalidate query cache.

            ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                ses.save(new Entity(5, "name-5"));

                tx.commit();
            }
            finally {
                ses.close();
            }

            // Run queries.

            ses = sesFactory1.openSession();

            sesFactory1.getStatistics().clear();

            try {
                Query qry1 = ses.createQuery("from " + ENTITY_NAME + " where id > 2");

                qry1.setCacheable(true);

                assertEquals(3, qry1.list().size());

                Query qry2 = ses.createQuery("from " + ENTITY_NAME + " where name = 'name-0'");

                qry2.setCacheable(true);

                assertEquals(1, qry2.list().size());
            }
            finally {
                ses.close();
            }

            assertEquals(0, sesFactory1.getStatistics().getQueryCacheHitCount());
            assertEquals(2, sesFactory1.getStatistics().getQueryCacheMissCount());
            assertEquals(2, sesFactory1.getStatistics().getQueryCachePutCount());

            // Clear query cache using another SessionFactory.

            sesFactory2.getCache().evictDefaultQueryRegion();

            ses = sesFactory1.openSession();

            // Run queries again.

            sesFactory1.getStatistics().clear();

            try {
                Query qry1 = ses.createQuery("from " + ENTITY_NAME + " where id > 2");

                qry1.setCacheable(true);

                assertEquals(3, qry1.list().size());

                Query qry2 = ses.createQuery("from " + ENTITY_NAME + " where name = 'name-0'");

                qry2.setCacheable(true);

                assertEquals(1, qry2.list().size());
            }
            finally {
                ses.close();
            }

            assertEquals(0, sesFactory1.getStatistics().getQueryCacheHitCount());
            assertEquals(2, sesFactory1.getStatistics().getQueryCacheMissCount());
            assertEquals(2, sesFactory1.getStatistics().getQueryCachePutCount());
        }
        finally {
            cleanup();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRegionClear() throws Exception {
        for (AccessType accessType : accessTypes())
            testRegionClear(accessType);
    }

    /**
     * @param accessType Cache access type.
     * @throws Exception If failed.
     */
    private void testRegionClear(AccessType accessType) throws Exception {
        createSessionFactories(accessType);

        try {
            final int ENTITY_CNT = 100;

            Session ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                for (int i = 0; i < ENTITY_CNT; i++) {
                    Entity e = new Entity(i, "name-" + i);

                    Collection<ChildEntity> children = new ArrayList<>();

                    for (int j = 0; j < 3; j++)
                        children.add(new ChildEntity());

                    e.setChildren(children);

                    ses.save(e);
                }

                tx.commit();
            }
            finally {
                ses.close();
            }

            loadEntities(sesFactory2, ENTITY_CNT);

            CacheRegionStatistics stats1 = sesFactory1.getStatistics().getCacheRegionStatistics(ENTITY_NAME);
            CacheRegionStatistics stats2 = sesFactory2.getStatistics().getCacheRegionStatistics(ENTITY_NAME);

            CacheRegionStatistics idStats1 =
                sesFactory1.getStatistics().getCacheRegionStatistics(NATURAL_ID_REGION);
            CacheRegionStatistics idStats2 =
                sesFactory2.getStatistics().getCacheRegionStatistics(NATURAL_ID_REGION);

            CacheRegionStatistics colStats1 =
                sesFactory1.getStatistics().getDomainDataRegionStatistics(CHILD_COLLECTION_REGION);
            CacheRegionStatistics colStats2 =
                sesFactory2.getStatistics().getDomainDataRegionStatistics(CHILD_COLLECTION_REGION);

            assertEquals(ENTITY_CNT, stats1.getElementCountInMemory());
            assertEquals(ENTITY_CNT, stats2.getElementCountInMemory());

            assertEquals(ENTITY_CNT, idStats1.getElementCountInMemory());
            assertEquals(ENTITY_CNT, idStats2.getElementCountInMemory());

            assertEquals(ENTITY_CNT, colStats1.getElementCountInMemory());
            assertEquals(ENTITY_CNT, colStats2.getElementCountInMemory());

            // Test cache is cleared after update query.

            ses = sesFactory1.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                ses.createQuery("delete from " + ENTITY_NAME + " where name='no such name'").executeUpdate();

                ses.createQuery("delete from " + ChildEntity.class.getName() + " where id=-1").executeUpdate();

                tx.commit();
            }
            finally {
                ses.close();
            }

            assertEquals(0, stats1.getElementCountInMemory());
            assertEquals(0, stats2.getElementCountInMemory());

            assertEquals(0, idStats1.getElementCountInMemory());
            assertEquals(0, idStats2.getElementCountInMemory());

            assertEquals(0, colStats1.getElementCountInMemory());
            assertEquals(0, colStats2.getElementCountInMemory());

            // Load some data in cache.

            loadEntities(sesFactory1, 10);

            assertEquals(10, stats1.getElementCountInMemory());
            assertEquals(10, stats2.getElementCountInMemory());
            assertEquals(10, idStats1.getElementCountInMemory());
            assertEquals(10, idStats2.getElementCountInMemory());

            // Test evictAll method.

            sesFactory2.getCache().evictEntityRegion(ENTITY_NAME);

            assertEquals(0, stats1.getElementCountInMemory());
            assertEquals(0, stats2.getElementCountInMemory());

            sesFactory2.getCache().evictNaturalIdRegion(ENTITY_NAME);

            assertEquals(0, idStats1.getElementCountInMemory());
            assertEquals(0, idStats2.getElementCountInMemory());

            sesFactory2.getCache().evictCollectionRegion(CHILD_COLLECTION_REGION);

            assertEquals(0, colStats1.getElementCountInMemory());
            assertEquals(0, colStats2.getElementCountInMemory());
        }
        finally {
            cleanup();
        }
    }

    /**
     * @param sesFactory Session factory.
     * @param nameToId Name-ID mapping.
     * @param absentNames Absent entities' names.
     */
    private void assertNaturalIdCache(SessionFactory sesFactory, Map<String, Integer> nameToId, String... absentNames) {
        sesFactory.getStatistics().clear();

        CacheRegionStatistics stats = sesFactory.getStatistics().getCacheRegionStatistics(NATURAL_ID_REGION);

        long hitBefore = stats.getHitCount();

        long missBefore = stats.getMissCount();

        final Session ses = sesFactory.openSession();

        try {
            for (Map.Entry<String, Integer> e : nameToId.entrySet())
                assertEquals((int) e.getValue(), ((Entity) ses.bySimpleNaturalId(Entity.class).load(e.getKey())).getId());

            for (String name : absentNames)
                assertNull((ses.bySimpleNaturalId(Entity.class).load(name)));

            assertEquals(nameToId.size() + hitBefore, stats.getHitCount());

            assertEquals(absentNames.length + missBefore, stats.getMissCount());
        }
        finally {
            ses.close();
        }
    }

    /**
     * @param sesFactory Session factory.
     * @param idToChildCnt Number of children per entity.
     * @param expHit Expected cache hits.
     * @param expMiss Expected cache misses.
     */
    @SuppressWarnings("unchecked")
    private void assertCollectionCache(SessionFactory sesFactory, Map<Integer, Integer> idToChildCnt, int expHit,
        int expMiss) {
        sesFactory.getStatistics().clear();

        Session ses = sesFactory.openSession();

        try {
            for(Map.Entry<Integer, Integer> e : idToChildCnt.entrySet()) {
                Entity entity = (Entity)ses.load(Entity.class, e.getKey());

                assertEquals((int)e.getValue(), entity.getChildren().size());
            }
        }
        finally {
            ses.close();
        }

        CacheRegionStatistics stats = sesFactory.getStatistics().getCacheRegionStatistics(CHILD_COLLECTION_REGION);

        assertEquals(expHit, stats.getHitCount());

        assertEquals(expMiss, stats.getMissCount());
    }

    /**
     * @param sesFactory Session factory.
     * @param cnt Number of entities to load.
     */
    private void loadEntities(SessionFactory sesFactory, int cnt) {
        Session ses = sesFactory.openSession();

        try {
            for (int i = 0; i < cnt; i++) {
                Entity e = (Entity)ses.load(Entity.class, i);

                assertEquals("name-" + i, e.getName());

                assertFalse(e.getChildren().isEmpty());

                ses.bySimpleNaturalId(Entity.class).load(e.getName());
            }
        }
        finally {
            ses.close();
        }
    }

    /**
     * @param entityName Entity name.
     * @param sesFactory Session factory.
     * @param idToName ID to name mapping.
     * @param absentIds Absent entities' IDs.
     */
    private void assertEntityCache(String entityName, SessionFactory sesFactory, Map<Integer, String> idToName,
                                   Integer... absentIds) {
        assert entityName.equals(ENTITY_NAME) || entityName.equals(ENTITY2_NAME) : entityName;

        sesFactory.getStatistics().clear();

        final Session ses = sesFactory.openSession();

        final boolean entity1 = entityName.equals(ENTITY_NAME);

        try {
            if (entity1) {
                for (Map.Entry<Integer, String> e : idToName.entrySet())
                    assertEquals(e.getValue(), ((Entity) ses.load(Entity.class, e.getKey())).getName());
                }
            else {
                for (Map.Entry<Integer, String> e : idToName.entrySet())
                    assertEquals(e.getValue(), ((Entity2) ses.load(Entity2.class, e.getKey())).getName());
            }

            for (final int id : absentIds) {
                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        if (entity1)
                            ((Entity) ses.load(Entity.class, id)).getName();
                        else
                            ((Entity2) ses.load(Entity2.class, id)).getName();

                        return null;
                    }
                }, ObjectNotFoundException.class, null);
            }

            CacheRegionStatistics stats = sesFactory.getStatistics().getCacheRegionStatistics(entityName);
            assertEquals(idToName.size(), stats.getHitCount());

            assertEquals(absentIds.length, stats.getMissCount());
        }
        finally {
            ses.close();
        }
    }

    /**
     * Creates session factories.
     *
     * @param accessType Cache access type.
     */
    private void createSessionFactories(AccessType accessType) {
        sesFactory1 = startHibernate(accessType, getTestIgniteInstanceName(0));

        sesFactory2 = startHibernate(accessType, getTestIgniteInstanceName(1));
    }

    /**
     * Starts Hibernate.
     *
     * @param accessType Cache access type.
     * @param igniteInstanceName Ignite instance name.
     * @return Session factory.
     */
    private SessionFactory startHibernate(org.hibernate.cache.spi.access.AccessType accessType, String igniteInstanceName) {
        StandardServiceRegistryBuilder builder = registryBuilder();

        for (Map.Entry<String, String> e : hibernateProperties(igniteInstanceName, accessType.name()).entrySet())
            builder.applySetting(e.getKey(), e.getValue());

        // Use the same cache for Entity and Entity2.
        builder.applySetting(REGION_CACHE_PROPERTY + ENTITY2_NAME, ENTITY_NAME);

        StandardServiceRegistry srvcRegistry = builder.build();

        MetadataSources metadataSources = new MetadataSources(srvcRegistry);

        for (Class entityClass : getAnnotatedClasses())
            metadataSources.addAnnotatedClass(entityClass);

        Metadata metadata = metadataSources.buildMetadata();

        for (PersistentClass entityBinding : metadata.getEntityBindings()) {
            if (!entityBinding.isInherited())
                ((RootClass) entityBinding).setCacheConcurrencyStrategy(accessType.getExternalName());
        }

        for (org.hibernate.mapping.Collection collectionBinding : metadata.getCollectionBindings())
            collectionBinding.setCacheConcurrencyStrategy(accessType.getExternalName());

        return metadata.buildSessionFactory();
    }

    /**
     * @return Entities classes.
     */
    private Class[] getAnnotatedClasses() {
        return new Class[]{Entity.class, Entity2.class, VersionedEntity.class, ChildEntity.class, ParentEntity.class};
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

        if (sesFactory2 != null)
            sesFactory2.close();

        sesFactory2 = null;

        for (IgniteCacheProxy<?, ?> cache : ((IgniteKernal)grid(0)).caches())
            cache.clear();
    }

    /**
     * @param igniteInstanceName Node name.
     * @param dfltAccessType Default cache access type.
     * @return Properties map.
     */
    static Map<String, String> hibernateProperties(String igniteInstanceName, String dfltAccessType) {
        Map<String, String> map = new HashMap<>();

        map.put(HBM2DDL_AUTO, "create");
        map.put(JPA_SHARED_CACHE_MODE, SharedCacheMode.ALL.name());
        map.put(GENERATE_STATISTICS, "true");
        map.put(USE_SECOND_LEVEL_CACHE, "true");
        map.put(USE_QUERY_CACHE, "true");
        map.put(CACHE_REGION_FACTORY, HibernateRegionFactory.class.getName());
        map.put(RELEASE_CONNECTIONS, "on_close");
        map.put(IGNITE_INSTANCE_NAME_PROPERTY, igniteInstanceName);
        map.put(DFLT_ACCESS_TYPE_PROPERTY, dfltAccessType);

        return map;
    }
}
