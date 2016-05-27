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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import java.io.Serializable;
import java.util.List;
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
    private static final int ORGS_COUNT = 10;

    /** Count of collocated objects. */
    private static final int PERS_AT_ORG_COUNT = 10_000;

    /** Test timeout. */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** Test end time. */
    final long endTime = System.currentTimeMillis() + TEST_TIMEOUT - 60_000;

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
        for (int i = GRID_CNT - RESTARTED_NODE_CNT; i < GRID_CNT; ++i) {
            final int restartGrid = i;
            GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get() && System.currentTimeMillis() < endTime) {
                        System.out.println("+++ STOP");
                        stopGrid(restartGrid);
                        Thread.sleep(100);
                        System.out.println("+++ START");
                        startGrid(restartGrid);
                    }
                    return null;
                }
            }, "restart-node-" + i);
        }

        while (System.currentTimeMillis() < endTime) {
            for (int orgId = 0; orgId < ORGS_COUNT; ++orgId) {
//            IgniteCache<Integer, Organization> orgCache = getPrimary(orgId).cache(Organization.class.getSimpleName());
                try {
                    grid(0).compute().affinityRun(Organization.class.getSimpleName(), orgId, new IgniteRunnable() {
                        @IgniteInstanceResource
                        private Ignite ignite;

                        @Override public void run() {
                            List res = ignite.cache(Organization.class.getSimpleName()).query(new SqlFieldsQuery(
                                "SELECT p.id FROM Person as p, Organization as o WHERE p.orgId=o.id").setLocal(true))
                                .getAll();

                            System.out.println("+++ RES size: " + res.size());
                        }
                    });
                } catch(IgniteException ex) {
                    // Swallow ignite exceptions
                }
            }
        }

        stop.set(true);
    }

    /**
     *
     */
    private Ignite getPrimary(Object key) {
        while (true) {
            try {
                for (int i = 0; i < GRID_CNT; i++) {
                    if (grid().affinity(
                        Organization.class.getSimpleName()).isPrimary(grid(i).cluster().localNode(), key))
                        return grid(i);
                }
            }
            catch (Exception ex) {
            }
        }
    }

    /**
     *
     */
    private void fillCaches() throws InterruptedException {
        grid(0).createCache(Organization.class.getSimpleName());
        grid(0).createCache(Person.class.getSimpleName());
        try (
            IgniteDataStreamer<Integer, Organization> orgStreamer =
                grid(0).dataStreamer(Organization.class.getSimpleName());
            IgniteDataStreamer<Person.Key, Person> persStreamer =
                grid(0).dataStreamer(Person.class.getSimpleName())) {

            int persId = 0;
            for (int orgId = 0; orgId < ORGS_COUNT; ++orgId) {
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
        @QuerySqlField
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

        @QuerySqlField
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

                if (id != key.id)
                    return false;
                return orgId == key.orgId;

            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                int result = id;
                result = 31 * result + orgId;
                return result;
            }
        }
    }
}