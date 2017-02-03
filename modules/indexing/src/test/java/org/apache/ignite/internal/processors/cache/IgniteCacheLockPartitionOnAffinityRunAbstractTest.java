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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Base class of the tests to validate https://issues.apache.org/jira/browse/IGNITE-2310
 */
public class IgniteCacheLockPartitionOnAffinityRunAbstractTest extends GridCacheAbstractSelfTest {
    /** Count of affinity run threads. */
    protected static final int AFFINITY_THREADS_CNT = 10;

    /** Count of collocated objects. */
    protected static final int PERS_AT_ORG_CNT = 10_000;

    /** Name of the cache with special affinity function (all partition are placed on the first node). */
    protected static final String OTHER_CACHE_NAME = "otherCache";

    /** Grid count. */
    protected static final int GRID_CNT = 4;

    /** Count of restarted nodes. */
    protected static final int RESTARTED_NODE_CNT = 2;

    /** Count of objects. */
    protected static final int ORGS_COUNT_PER_NODE = 2;

    /** Test duration. */
    protected static final long TEST_DURATION = 5 * 60_000;

    /** Test timeout. */
    protected static final long TEST_TIMEOUT = TEST_DURATION + 2 * 60_000;

    /** Timeout between restart of a node. */
    protected static final long RESTART_TIMEOUT = 3_000;

    /** Max failover attempts. */
    protected static final int MAX_FAILOVER_ATTEMPTS = 100;

    /** Organization ids. */
    protected static List<Integer> orgIds;

    /** Test end time. */
    protected static long endTime;

    /** Node restart thread future. */
    protected static IgniteInternalFuture<?> nodeRestartFut;

    /** Stop a test flag . */
    protected final AtomicBoolean stopRestartThread = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setMarshaller(new BinaryMarshaller());

        AlwaysFailoverSpi failSpi = new AlwaysFailoverSpi();
        failSpi.setMaximumFailoverAttempts(MAX_FAILOVER_ATTEMPTS);
        cfg.setFailoverSpi(failSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[] {
            Integer.class, Organization.class,
            Person.Key.class, Person.class,
            Integer.class, Integer.class
        };
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        info("Fill caches begin...");

        fillCaches();

        info("Caches are filled");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        grid(0).destroyCache(Organization.class.getSimpleName());
        grid(0).destroyCache(Person.class.getSimpleName());
        grid(0).destroyCache(OTHER_CACHE_NAME);
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopRestartThread.set(true);
        if (nodeRestartFut != null) {
            nodeRestartFut.get();
            nodeRestartFut = null;
        }

        Thread.sleep(3_000);

        awaitPartitionMapExchange();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        endTime = System.currentTimeMillis() + TEST_DURATION;

        super.beforeTest();
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void createCacheWithAffinity(String cacheName) throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(grid(0).name());
        ccfg.setName(cacheName);

        ccfg.setAffinity(new DummyAffinity());

        grid(0).createCache(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    private void fillCaches() throws Exception {
        grid(0).createCache(Organization.class.getSimpleName());
        grid(0).createCache(Person.class.getSimpleName());

        createCacheWithAffinity(OTHER_CACHE_NAME);

        awaitPartitionMapExchange();

        orgIds = new ArrayList<>(ORGS_COUNT_PER_NODE * RESTARTED_NODE_CNT);

        for (int i = GRID_CNT - RESTARTED_NODE_CNT; i < GRID_CNT; ++i)
            orgIds.addAll(primaryKeys(grid(i).cache(Organization.class.getSimpleName()), ORGS_COUNT_PER_NODE));

        try (
            IgniteDataStreamer<Integer, Organization> orgStreamer =
                grid(0).dataStreamer(Organization.class.getSimpleName());
            IgniteDataStreamer<Person.Key, Person> persStreamer =
                grid(0).dataStreamer(Person.class.getSimpleName())) {

            int persId = 0;
            for (int orgId : orgIds) {
                Organization org = new Organization(orgId);
                orgStreamer.addData(orgId, org);

                for (int persCnt = 0; persCnt < PERS_AT_ORG_CNT; ++persCnt, ++persId) {
                    Person pers = new Person(persId, orgId);
                    persStreamer.addData(pers.createKey(), pers);
                }
            }
        }

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    protected void beginNodesRestart() {
        stopRestartThread.set(false);
        nodeRestartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int restartGrid = GRID_CNT - RESTARTED_NODE_CNT;
                while (!stopRestartThread.get() && System.currentTimeMillis() < endTime) {
                    log.info("Restart grid: " + restartGrid);
                    stopGrid(restartGrid);
                    Thread.sleep(500);
                    startGrid(restartGrid);

                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return !stopRestartThread.get();
                        }
                    }, RESTART_TIMEOUT);

                    restartGrid++;
                    if (restartGrid >= GRID_CNT)
                        restartGrid = GRID_CNT - RESTARTED_NODE_CNT;
                    awaitPartitionMapExchange();
                }
                return null;
            }
        }, "restart-node");
    }

    /**
     * @param ignite Ignite.
     * @param orgId Org id.
     * @param expReservations Expected reservations.
     * @throws Exception If failed.
     */
    protected static void checkPartitionsReservations(final IgniteEx ignite, int orgId,
        final int expReservations) throws Exception {
        int part = ignite.affinity(Organization.class.getSimpleName()).partition(orgId);

        final GridDhtLocalPartition pPers = ignite.context().cache()
            .internalCache(Person.class.getSimpleName()).context().topology()
            .localPartition(part, AffinityTopologyVersion.NONE, false);

        assertNotNull(pPers);

        final GridDhtLocalPartition pOrgs = ignite.context().cache()
            .internalCache(Organization.class.getSimpleName()).context().topology()
            .localPartition(part, AffinityTopologyVersion.NONE, false);

        assertNotNull(pOrgs);
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expReservations == pOrgs.reservations() && expReservations == pPers.reservations();
            }
        }, 1000L);
        assertEquals("Unexpected reservations count", expReservations, pOrgs.reservations());
        assertEquals("Unexpected reservations count", expReservations, pPers.reservations());
    }

    /** */
    private static class DummyAffinity extends RendezvousAffinityFunction {
        /**
         * Default constructor.
         */
        public DummyAffinity() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            List<List<ClusterNode>> assign = new ArrayList<>(partitions());

            for (int i = 0; i < partitions(); ++i)
                assign.add(Collections.singletonList(nodes.get(0)));

            return assign;
        }
    }


    /**
     * Test class Organization.
     */
    public static class Organization implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private final int id;

        /**
         * @param id ID.
         */
        Organization(int id) {
            this.id = id;
        }

        /**
         * @return id.
         */
        int getId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Organization.class, this);
        }
    }

    /**
     * Test class Organization.
     */
    public static class Person implements Serializable {
        /** */
        @QuerySqlField
        private final int id;

        /** */
        @QuerySqlField(index = true)
        private final int orgId;

        /**
         * @param id ID.
         * @param orgId Organization ID.
         */
        Person(int id, int orgId) {
            this.id = id;
            this.orgId = orgId;
        }

        /**
         * @return id.
         */
        int getId() {
            return id;
        }

        /**
         * @return organization id.
         */
        int getOrgId() {
            return orgId;
        }

        /**
         * @return Affinity key.
         */
        public Person.Key createKey() {
            return new Person.Key(id, orgId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /**
         *
         */
        static class Key implements Serializable {
            /** Id. */
            private final int id;

            /** Org id. */
            @AffinityKeyMapped
            protected final int orgId;

            /**
             * @param id Id.
             * @param orgId Org id.
             */
            private Key(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;

                Person.Key key = (Person.Key)o;

                return id == key.id && orgId == key.orgId;
            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                int res = id;
                res = 31 * res + orgId;
                return res;
            }
        }
    }
}
