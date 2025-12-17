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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.jupiter.api.Test;

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

    /** */
    private boolean pds;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failureHnd = new TestFailureHandler(true);
        validateTypes = false;
        indexedKeyCls = BaseKey.class;
        pds = false;

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
                .setPersistenceEnabled(pds)));

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
    @Test
    public void testIncompatibleAffinityField() throws Exception {
        testDifferentKeyTypes(false, IncompatibleAffinityFieldKey::new, true, false);
    }

    /** */
    @Test
    public void testConcurrentIndexRecreate() throws Exception {
        startGrid(0);

        AtomicBoolean stop = new AtomicBoolean();

        sql(grid(0), "CREATE INDEX IDX_NAME ON \"" + DEFAULT_CACHE_NAME + "\".TESTVALUE(NAME)");

        GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                sql(grid(0), "CREATE INDEX IDX ON \"" + DEFAULT_CACHE_NAME + "\".TESTVALUE(UID)");
                sql(grid(0), "DROP INDEX \"" + DEFAULT_CACHE_NAME + "\".IDX");
            }
        });

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < 100_000; i++) {
                Object key = new BaseKey(i);
                cache.put(key, new TestValue(UUID.randomUUID(), Integer.toString(i)));
                cache.remove(key);
            }
        }
        finally {
            stop.set(true);
        }

        assertEquals(0, cache.size());
    }

    /** */
    @Test
    public void testPrimitiveKey() throws Exception {
        indexedKeyCls = Integer.class;

        Ignite ignite = startGrid(0);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.put(0, new TestValue(UUID.randomUUID(), "0"));

        GridTestUtils.assertThrowsWithCause(() -> cache.put(new BaseKey(0), new TestValue(UUID.randomUUID(), "0")),
            CacheException.class);

        GridTestUtils.assertThrowsWithCause(() -> cache.put("0", new TestValue(UUID.randomUUID(), "0")),
            CacheException.class);

        assertEquals(1, cache.size());
        assertEquals(1,
            sql(grid(0), "select _KEY, name from \"" + DEFAULT_CACHE_NAME + "\".TESTVALUE").size());
    }

    /** */
    @Test
    public void testPrimitiveKeyWithAlias() throws Exception {
        Ignite ignite = startGrid(0);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("KEY", Integer.class.getName());
        fields.put("UID", UUID.class.getName());
        fields.put("NAME", String.class.getName());

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(new CacheConfiguration<>("cache")
            .setQueryEntities(Collections.singletonList(
                new QueryEntity()
                    .setKeyType(Integer.class.getName())
                    .setValueType(TestValue.class.getName())
                    .setFields(fields)
                    .setKeyFieldName("KEY")
                    .setAliases(F.asMap("KEY", "ID"))
        )));

        cache.put(0, new TestValue(UUID.randomUUID(), "0"));

        GridTestUtils.assertThrowsWithCause(() -> cache.put(new BaseKey(0), new TestValue(UUID.randomUUID(), "0")),
            CacheException.class);

        GridTestUtils.assertThrowsWithCause(() -> cache.put("0", new TestValue(UUID.randomUUID(), "0")),
            CacheException.class);

        assertEquals(1, cache.size());
        assertEquals(1,
            sql(grid(0), "select id, name from \"cache\".TESTVALUE").size());
    }

    /** */
    private void testDifferentKeyTypes(
        boolean restart,
        IntFunction<Object> keyFactory,
        boolean expFailureOnPut,
        boolean expFailureOnSelect
    ) throws Exception {
        pds = restart;

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
