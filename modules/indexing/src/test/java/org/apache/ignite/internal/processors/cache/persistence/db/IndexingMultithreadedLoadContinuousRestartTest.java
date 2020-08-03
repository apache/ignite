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
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that continuous non-graceful node stop under load doesn't break SQL indexes.
 */
public class IndexingMultithreadedLoadContinuousRestartTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache-name";

    /** Restarts. */
    private static final int RESTARTS = 5;

    /** Load threads. */
    private static final int THREADS = 5;

    /** Load loop cycles. */
    private static final int LOAD_LOOP = 5000;

    /** Key bound. */
    private static final int KEY_BOUND = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(UserKey.class.getName());
        qryEntity.setValueType(UserValue.class.getName());
        qryEntity.setTableName("USER_TEST_TABLE");
        qryEntity.setKeyFields(new HashSet<>(Arrays.asList("a", "b", "c")));

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("a", "java.lang.Integer");
        fields.put("b", "java.lang.Integer");
        fields.put("c", "java.lang.Integer");
        fields.put("x", "java.lang.Integer");
        fields.put("y", "java.lang.Integer");
        fields.put("z", "java.lang.Integer");
        qryEntity.setFields(fields);

        QueryIndex idx1 = new QueryIndex();
        idx1.setName("IDX_1");
        idx1.setIndexType(QueryIndexType.SORTED);
        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();
        idxFields.put("a", false);
        idxFields.put("b", false);
        idxFields.put("c", false);
        idx1.setFields(idxFields);

        QueryIndex idx2 = new QueryIndex();
        idx2.setName("IDX_2");
        idx2.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("x", false);
        idx2.setFields(idxFields);

        QueryIndex idx3 = new QueryIndex();
        idx3.setName("IDX_3");
        idx3.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("y", false);
        idx3.setFields(idxFields);

        QueryIndex idx4 = new QueryIndex();
        idx4.setName("IDX_4");
        idx4.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("z", false);
        idx4.setFields(idxFields);

        qryEntity.setIndexes(Arrays.asList(idx1, idx2, idx3, idx4));

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, Integer>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setName(CACHE_NAME)
            .setQueryEntities(Collections.singleton(qryEntity)));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(200L * 1024 * 1024)
                        .setMaxSize(200L * 1024 * 1024)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * Tests that continuous non-graceful node stop under load doesn't break SQL indexes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        for (int i = 0; i < RESTARTS; i++) {
            IgniteEx ignite = startGrid(0);

            ignite.cluster().active(true);

            // Ensure that checkpoint isn't running - otherwise validate indexes task may fail.
            forceCheckpoint();

            // Validate indexes on start.
            ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(CACHE_NAME), 0, 0, false, true);
            ignite.context().resource().injectGeneric(clo);
            VisorValidateIndexesJobResult res = clo.call();

            assertFalse(res.hasIssues());

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    IgniteCache<UserKey, UserValue> cache = ignite.cache(CACHE_NAME);

                    int i = 0;
                    try {
                        for (; i < LOAD_LOOP; i++) {
                            ThreadLocalRandom r = ThreadLocalRandom.current();

                            Integer keySeed = r.nextInt(KEY_BOUND);
                            UserKey key = new UserKey(keySeed);

                            if (r.nextBoolean())
                                cache.put(key, new UserValue(r.nextLong()));
                            else
                                cache.remove(key);
                        }

                        ignite.close(); // Intentionally stop grid while another loaders are still in progress.
                    }
                    catch (Exception e) {
                        log.warning("Failed to update cache after " + i + " loop cycles", e);
                    }
                }
            }, THREADS, "loader");

            fut.get();

            ignite.close();
        }
    }

    /**
     * User key.
     */
    private static class UserKey {
        /** A. */
        private int a;

        /** B. */
        private int b;

        /** C. */
        private int c;

        /**
         * @param a A.
         * @param b B.
         * @param c C.
         */
        public UserKey(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        /**
         * @param seed Seed.
         */
        public UserKey(long seed) {
            a = (int)(seed % 17);
            b = (int)(seed % 257);
            c = (int)(seed % 3001);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "UserKey{" +
                "a=" + a +
                ", b=" + b +
                ", c=" + c +
                '}';
        }
    }

    /**
     * User value.
     */
    private static class UserValue {
        /** X. */
        private int x;

        /** Y. */
        private int y;

        /** Z. */
        private int z;

        /**
         * @param x X.
         * @param y Y.
         * @param z Z.
         */
        public UserValue(int x, int y, int z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        /**
         * @param seed Seed.
         */
        public UserValue(long seed) {
            x = (int)(seed % 6991);
            y = (int)(seed % 18679);
            z = (int)(seed % 31721);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "UserValue{" +
                "x=" + x +
                ", y=" + y +
                ", z=" + z +
                '}';
        }
    }
}
