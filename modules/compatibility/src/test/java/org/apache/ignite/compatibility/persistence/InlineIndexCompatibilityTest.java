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

package org.apache.ignite.compatibility.persistence;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/**
 * Tests that upgrade version on persisted inline index is successfull.
 */
public class InlineIndexCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = InlineIndexCompatibilityTest.class.getSimpleName();

    /** */
    @Test
    public void test_2_8_1() throws Exception {
        doTestStartupWithOldVersion("2.8.1");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
                // Disable WAL to skip filling index with reading WAL. Instead just start on previous persisted files.
                .setWalMode(WALMode.NONE));

        cfg.setBinaryConfiguration(
            new BinaryConfiguration()
                .setCompactFooter(true)
        );

        return cfg;
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    protected void doTestStartupWithOldVersion(String igniteVer) throws Exception {
        try {
            startGrid(1, igniteVer,
                new PersistenceBasicCompatibilityTest.ConfigurationClosure(true),
                new PostStartupClosure());

            stopAllGrids();

            Thread.sleep(100);

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            Thread.sleep(100);

            validateResultingCacheData(ignite.cache(TEST_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache to be filled by different keys and values. Results may be validated in {@link
     * #validateResultingCacheData(IgniteCache)}.
     */
    public static void saveCacheData(IgniteCache<Object, Object> cache) {
        for (int i = 0; i < 100; i++)
            cache.put(i, new EntityValueValue(new EntityValue(i + 2), i, i + 1));

        cache.query(new SqlFieldsQuery(
            "CREATE INDEX intval1_val_intval2 ON \"" + TEST_CACHE_NAME + "\".EntityValueValue " +
                "(intVal1, value, intVal2)")).getAll();
    }

    /**
     * Asserts cache contained all expected values as it was saved before.
     *
     * @param cache Cache  should be filled using {@link #saveCacheData(IgniteCache)}.
     */
    public static void validateResultingCacheData(IgniteCache<Object, Object> cache) {
        List<List<?>> result = cache.query(
            // Select by quering complex index.
            new SqlFieldsQuery(
                "SELECT * FROM \"" + TEST_CACHE_NAME + "\".EntityValueValue v " +
                    "WHERE v.intVal1 = ? and v.value = ? and v.intVal2 = ?;")
                .setArgs(12, new EntityValue(14), 13)
        ).getAll();

        System.out.println("RESULT: " + result);

        assertTrue(result.size() == 1);

        List<?> row = result.get(0);

        assertTrue(row.get(0).equals(new EntityValue(14)));
        assertTrue(row.get(1).equals(12));
        assertTrue(row.get(2).equals(13));
    }

    /** */
    public static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);

            cacheCfg.setIndexedTypes(Integer.class, EntityValueValue.class);

            IgniteCache<Object, Object> cache = ignite.createCache(cacheCfg);

            saveCacheData(cache);

            ignite.active(false);

            try {
                Thread.sleep(1_000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /** */
    public static class EntityValue {
        private int value;

        public EntityValue(int value) {
            this.value = value;
        }

        @Override public String toString() {
            return "EV[value=" + value + "]";
        }

        @Override public int hashCode() {
            return 1 + value;
        }

        @Override public boolean equals(Object other) {
            return value == ((EntityValue) other).value;
        }
    }

    public static class EntityValueValue {

        @QuerySqlField(index = true)
        private EntityValue value;

        @QuerySqlField
        private int intVal1;

        @QuerySqlField
        private int intVal2;

        public EntityValueValue(EntityValue value) {
            this(value, 0, 0);
        }

        public EntityValueValue(EntityValue value, int val1, int val2) {
            this.value = value;
            intVal1 = val1;
            intVal2 = val2;
        }

        @Override public String toString() {
            return "EVV[value=" + value + "]";
        }
    }
}
