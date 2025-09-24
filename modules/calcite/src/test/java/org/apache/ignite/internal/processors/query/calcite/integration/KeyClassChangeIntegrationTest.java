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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.UUID;
import java.util.function.IntFunction;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

/** */
public class KeyClassChangeIntegrationTest extends AbstractMultiEngineIntegrationTest {
    /** */
    public static final int SRV_CNT = 3;

    /** */
    private TestFailureHandler failureHnd;

    /** */
    private boolean validateTypes;

    /** */
    private Class<?> indexedKeyCls;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failureHnd = new TestFailureHandler(true);
        validateTypes = false;
        indexedKeyCls = BaseKey.class;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        cfg.setFailureHandler(failureHnd);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setIndexedTypes(indexedKeyCls, TestValue.class));

        cfg.getSqlConfiguration().setValidationEnabled(validateTypes);

        return cfg;
    }

    /** */
    @Test
    public void testCompatibleKeys() throws Exception {
        testDifferentKeyTypes(false, CompatibleKey::new, false, false);
    }

    /** */
    @Test
    public void testCompatibleKeysWithRestart() throws Exception {
        testDifferentKeyTypes(true, CompatibleKey::new, false, false);
    }

    /** */
    @Test
    public void testIncompatibleFieldWithValidation() throws Exception {
        validateTypes = true;

        testDifferentKeyTypes(false, IncompatibleFieldKey::new, true, false);
    }

    /** */
    @Test
    public void testIncompatibleFieldWithoutValidation() throws Exception {
        testDifferentKeyTypes(false, IncompatibleFieldKey::new, false, true);
    }

    /** */
    @Test
    public void testIncompatibleIndexedField() throws Exception {
        indexedKeyCls = IndexedFieldKey.class;

        testDifferentKeyTypes(false, IncompatibleFieldKey::new, true, false);
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-26467")
    @Test
    public void testIncompatibleAffinityField() throws Exception {
        testDifferentKeyTypes(false, IncompatibleAffinityFieldKey::new, true, false);
    }

    /** */
    private void testDifferentKeyTypes(
        boolean restart,
        IntFunction<Object> keyFactory,
        boolean expFailureOnPut,
        boolean expFailureOnSelect
    ) throws Exception {
        startGrids(SRV_CNT).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        int keyCnt = 10;

        for (int i = 0; i < keyCnt; i++)
            cache0.put(new BaseKey(i), new TestValue(UUID.randomUUID(), "0"));

        checkSize(keyCnt);

        if (restart) {
            grid(0).cluster().state(ClusterState.INACTIVE);

            stopAllGrids();

            startGrids(SRV_CNT);
        }

        IgniteCache<Object, Object> cache1 = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < keyCnt; i++) {
            Object key = keyFactory.apply(i);
            Object val = new TestValue(UUID.randomUUID(), "1");

            if (expFailureOnPut)
                GridTestUtils.assertThrowsWithCause(() -> cache1.put(key, val), CacheException.class);
            else
                cache1.put(key, val);
        }

        int expSize = keyCnt * (expFailureOnPut ? 1 : 2);

        if (expFailureOnSelect)
            GridTestUtils.assertThrowsWithCause(() -> checkSize(expSize), Exception.class);
        else
            checkSize(expSize);

        assertNull("Nodes failed", failureHnd.awaitFailure(1_000L));
    }

    /** */
    private void checkSize(int exp) {
        assertEquals("Unexpected cache size", exp, grid(0).cache(DEFAULT_CACHE_NAME).size());
        assertEquals("Cache size and SQL size differs", exp,
            sql(grid(0), "select kuid, name from \"" + DEFAULT_CACHE_NAME + "\".TESTVALUE order by key").size());
    }

    /** */
    private static class TestValue {
        /** */
        @QuerySqlField
        private final UUID uid;

        /** */
        @QuerySqlField
        private final String name;

        /** */
        private TestValue(UUID uid, String name) {
            this.uid = uid;
            this.name = name;
        }
    }

    /** */
    private static class BaseKey {
        /** */
        @AffinityKeyMapped
        @QuerySqlField
        private final int key;

        /** */
        @QuerySqlField
        private final UUID kuid = UUID.randomUUID();

        /** */
        private BaseKey(int key) {
            this.key = key;
        }
    }

    /** */
    private static class CompatibleKey {
        /** */
        @AffinityKeyMapped
        @QuerySqlField
        private final int key;

        /** */
        @QuerySqlField
        private final UUID kuid = UUID.randomUUID();

        /** */
        private CompatibleKey(int key) {
            this.key = key;
        }
    }

    /** */
    private static class IndexedFieldKey {
        /** */
        @AffinityKeyMapped
        @QuerySqlField
        private final int key;

        /** */
        @QuerySqlField(index = true)
        private final UUID kuid = UUID.randomUUID();

        /** */
        private IndexedFieldKey(int key) {
            this.key = key;
        }
    }

    /** */
    private static class IncompatibleFieldKey {
        /** */
        @AffinityKeyMapped
        @QuerySqlField
        private final int key;

        /** */
        @QuerySqlField
        private final String kuid = UUID.randomUUID().toString();

        /** */
        private IncompatibleFieldKey(int key) {
            this.key = key;
        }
    }

    /** */
    private static class IncompatibleAffinityFieldKey {
        /** */
        @AffinityKeyMapped
        @QuerySqlField
        private final long key;

        /** */
        @QuerySqlField
        private final UUID kuid = UUID.randomUUID();

        /** */
        private IncompatibleAffinityFieldKey(int key) {
            this.key = key;
        }
    }
}
