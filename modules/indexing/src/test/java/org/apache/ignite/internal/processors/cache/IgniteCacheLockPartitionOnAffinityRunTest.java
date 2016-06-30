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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import javax.cache.CacheException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 */
public class IgniteCacheLockPartitionOnAffinityRunTest extends GridCacheAbstractSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** Count of restarted nodes. */
    private static final int RESTARTED_NODE_CNT = 2;

    /** Count of objects. */
    private static final int ORGS_COUNT_PER_NODE = 2;

    /** Count of collocated objects. */
    private static final int PERS_AT_ORG_COUNT = 10_000;

    /** Test timeout. */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;
    /** Test end time. */
    final long endTime = System.currentTimeMillis() + TEST_TIMEOUT - 60_000;
    private List<Integer> orgIds;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[] {
            Integer.class, Organization.class,
            Person.Key.class, Person.class
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

    /**
     * @throws Exception If failed.
     */
    public void testLockPartitionOnAffinityRunTest() throws Exception {
        info("Fill caches begin...");
        fillCaches();
        info("Caches are filled");

        final AtomicBoolean stop = new AtomicBoolean();

        // Run restart threads: start re-balancing
        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int restartGrid = GRID_CNT - RESTARTED_NODE_CNT;
                while (!stop.get() && System.currentTimeMillis() < endTime) {
                    stopGrid(restartGrid);
                    startGrid(restartGrid);
                    Thread.sleep(1000);
                    restartGrid++;
                    if (restartGrid >= GRID_CNT)
                        restartGrid = GRID_CNT - RESTARTED_NODE_CNT;
                }
                return null;
            }
        }, "restart-node");

        while (System.currentTimeMillis() < endTime) {
            for (final int orgId : orgIds) {
                try {
//                    List res = grid(0).cache(Person.class.getSimpleName()).query(new SqlFieldsQuery(
//                        String.format("SELECT * FROM \"%s\".Person as p, \"%s\".Organization as o " +
//                            "WHERE p.orgId=o.id " +
//                            "AND p.orgId=" + orgId,
//                            Person.class.getSimpleName(), Organization.class.getSimpleName()))).getAll();

                    grid(0).compute().affinityRun(Organization.class.getSimpleName(), orgId, new IgniteRunnable() {
                        @IgniteInstanceResource
                        private IgniteEx ignite;

                        @Override public void run() {
                            System.out.println("+++ RUN on: " + ignite.name());
                            List res = ignite.cache(Person.class.getSimpleName())
                                .query(new SqlFieldsQuery(
                                    String.format("SELECT * FROM \"%s\".Person as p, \"%s\".Organization as o " +
                                            "WHERE p.orgId = o.id " +
                                            "AND p.orgId = " + orgId,
                                        Person.class.getSimpleName(), Organization.class.getSimpleName())).setLocal(true))
                                .getAll();

                            try {
                                assertEquals(PERS_AT_ORG_COUNT, res.size());
                            } catch( Throwable t) {
//                                if (PERS_AT_ORG_COUNT != res.size()) {
                                    int part = ignite.affinity(Organization.class.getSimpleName()).partition(orgId);

                                    GridCacheAdapter<?, ?> cacheAdapterOrg = ignite.context().cache().internalCache(Organization.class.getName());
                                    GridCacheAdapter<?, ?> cacheAdapterPers = ignite.context().cache().internalCache(Person.class.getName());

                                    System.out.println("+++ ORG part: " + cacheAdapterOrg.context().topology().localPartition(part, AffinityTopologyVersion.NONE, false));
                                    System.out.println("+++ PERS part: " + cacheAdapterPers.context().topology().localPartition(part, AffinityTopologyVersion.NONE, false));
                                    assertTrue(false);
//                                }
                            }
                        }
                    }, lockedParts(orgId));

//                    System.out.println("+++ RES: " + res.size());
                }
                catch (IgniteException | IllegalStateException | CacheException ex) {
                    // Swallow exceptions in case node is shutdown
                }
            }
        }

        stop.set(true);
    }

    private Map<String, int[]> lockedParts(Object key) {
        Map<String, int[]> map = new HashMap<>();

        int[] parts = new int[] {grid(0).affinity(Organization.class.getSimpleName()).partition(key)};
        map.put(Organization.class.getSimpleName(),
            parts);
        map.put(Person.class.getSimpleName(),
            parts);

//        ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(key);

//        map.put(Person.class.getSimpleName(),
//            grid(0).affinity(Person.class.getSimpleName()).primaryPartitions(node));

        return map;
    }

    /**
     *
     */
    private void fillCaches() throws InterruptedException {
        grid(0).createCache(Organization.class.getSimpleName());
        grid(0).createCache(Person.class.getSimpleName());

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

                for (int persCnt = 0; persCnt < PERS_AT_ORG_COUNT; ++persCnt, ++persId) {
                    Person pers = new Person(persId, orgId);
                    persStreamer.addData(pers.createKey(), pers);
                }
            }
        }
        awaitPartitionMapExchange();
    }

    /**
     * Test class Organization.
     */
    private static class Organization implements Serializable {
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
    private static class Person implements Serializable {
        /** */
        @QuerySqlField
        private final int id;

        @QuerySqlField(index = true)
        private final int orgId;

        /**
         * @param id ID.
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
         *
         */
        public Key createKey() {
            return new Key(id, orgId);
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
            private final int orgId;

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

                Key key = (Key)o;

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