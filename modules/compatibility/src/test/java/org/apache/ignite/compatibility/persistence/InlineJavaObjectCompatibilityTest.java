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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_12_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_6_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.since;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/**
 * Tests that upgrade version on persisted inline index is successfull.
 */
@RunWith(Parameterized.class)
public class InlineJavaObjectCompatibilityTest extends IndexAbstractCompatibilityTest {
    /** */
    private static final String TEST_CACHE_NAME = InlineJavaObjectCompatibilityTest.class.getSimpleName();

    /** */
    private static final int ROWS_CNT = 1000;

    /** Index to test. */
    private static final String INDEX_NAME = "intval1_val_intval2";

    /** Index to test with configured inline size. */
    private static final String INDEX_SIZED_NAME = "intval1_val_intval2_sized";

    /** Parametrized run param: Ignite version. */
    @Parameterized.Parameter(0)
    public String igniteVer;

    /** Parametrized run param: Inline size is configured by user. */
    @Parameterized.Parameter(1)
    public boolean cfgInlineSize;

    /** Test run configurations: Ignite version, Inline size configuration. */
    @Parameterized.Parameters(name = "ver={0}, cfgInlineSize={1}")
    public static Collection<Object[]> runConfig() {
        /** 2.6.0 is a last version where POJO inlining isn't enabled. */
        return cartesianProduct(since(VER_2_6_0), F.asList(false, true));
    }

    /** */
    @Test
    public void testQueryOldInlinedIndex() throws Exception {
        int majorJavaVer = U.majorJavaVersion(U.jdkVersion());

        if (majorJavaVer > 11) {
            Assume.assumeTrue("Skipped on jdk " + U.jdkVersion(),
                VER_2_12_0.compareTo(IgniteReleasedVersion.fromString(igniteVer)) < 0);
        }

        PostStartupClosure closure = cfgInlineSize ? new PostStartupClosureSized() : new PostStartupClosure();
        String idxName = cfgInlineSize ? INDEX_SIZED_NAME : INDEX_NAME;

        doTestStartupWithOldVersion(igniteVer, closure, idxName);
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    protected void doTestStartupWithOldVersion(String igniteVer, PostStartupClosure closure, String idxName) throws Exception {
        try {
            startGrid(1, igniteVer,
                new PersistenceBasicCompatibilityTest.ConfigurationClosure(true),
                closure);

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.cluster().state(ClusterState.ACTIVE);

            validateResultingCacheData(ignite.cache(TEST_CACHE_NAME), idxName);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Asserts cache contained all expected values as it was saved before.
     *
     * @param cache Cache to check.
     * @param idxName Name of index to check.
     */
    private void validateResultingCacheData(IgniteCache<Object, Object> cache, String idxName) {
        validateRandomRow(cache, idxName);
        validateRandomRange(cache, idxName);
    }

    /** */
    private void validateRandomRow(IgniteCache<Object, Object> cache, String idxName) {
        int val = new Random().nextInt(ROWS_CNT);

        // Select by quering complex index.
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "SELECT * FROM \"" + TEST_CACHE_NAME + "\".EntityValueValue v " +
                "WHERE v.intVal1 = ? and v.val = ? and v.intVal2 = ?;")
            .setArgs(val, new EntityValue(val + 2), val + 1);

        checkIndexUsed(cache, qry, idxName);

        List<List<?>> result = cache.query(qry).getAll();

        assertTrue(result.size() == 1);

        List<?> row = result.get(0);

        assertTrue(row.get(0).equals(new EntityValue(val + 2)));
        assertTrue(row.get(1).equals(val));
        assertTrue(row.get(2).equals(val + 1));
    }

    /** */
    private void validateRandomRange(IgniteCache<Object, Object> cache, String idxName) {
        int pivot = new Random().nextInt(ROWS_CNT);

        // Select by quering complex index.
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "SELECT * FROM \"" + TEST_CACHE_NAME + "\".EntityValueValue v " +
                "WHERE v.intVal1 > ? and v.val > ? and v.intVal2 > ? " +
                "ORDER BY v.val, v.intVal1, v.intVal2;")
            .setArgs(pivot, new EntityValue(pivot), pivot);

        checkIndexUsed(cache, qry, idxName);

        List<List<?>> result = cache.query(qry).getAll();

        // For strict comparison. There was an issues with >= comparison for some versions.
        pivot += 1;

        assertTrue("Exp=" + (ROWS_CNT - pivot) + "; act=" + result.size(), result.size() == ROWS_CNT - pivot);

        for (int i = 0; i < ROWS_CNT - pivot; i++) {
            List<?> row = result.get(i);

            assertTrue(row.get(0).equals(new EntityValue(pivot + i + 2)));
            assertTrue(row.get(1).equals(pivot + i));
            assertTrue(row.get(2).equals(pivot + i + 1));
        }
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

        /**
         * Create a complex index (int, pojo, int). Check that middle POJO object is correctly available from inline.
         *
         * @param cache to be filled with data. Results may be validated in {@link #validateResultingCacheData(IgniteCache, String)}.
         */
        protected void saveCacheData(IgniteCache<Object, Object> cache) {
            for (int i = 0; i < ROWS_CNT; i++)
                cache.put(i, new EntityValueValue(new EntityValue(i + 2), i, i + 1));

            // Create index (int, pojo, int).
            cache.query(new SqlFieldsQuery(
                    "CREATE INDEX " + INDEX_NAME + " ON \"" + TEST_CACHE_NAME + "\".EntityValueValue " +
                    "(intVal1, val, intVal2)")).getAll();
        }
    }

    /** */
    public static class PostStartupClosureSized extends PostStartupClosure {
        /** {@inheritDoc} */
        @Override protected void saveCacheData(IgniteCache<Object, Object> cache) {
            for (int i = 0; i < ROWS_CNT; i++)
                cache.put(i, new EntityValueValue(new EntityValue(i + 2), i, i + 1));

            // Create index (int, pojo, int) with configured inline size.
            cache.query(new SqlFieldsQuery(
                "CREATE INDEX " + INDEX_SIZED_NAME + " ON \"" + TEST_CACHE_NAME + "\".EntityValueValue " +
                    "(intVal1, val, intVal2) " +
                    "INLINE_SIZE 100")).getAll();
        }
    }

    /** POJO object aimed to be inlined. */
    public static class EntityValue implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int val;

        /** */
        public EntityValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "EV[value=" + val + "]";
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object other) {
            return val == ((EntityValue)other).val;
        }

        /** Enable comparison of EntityValue objects by the {@link #val} field. */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(val);
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException {
            val = in.readInt();
        }
    }

    /** Represents a cache value with 3 fields (POJO, int, int). */
    public static class EntityValueValue {
        /** */
        @QuerySqlField
        private final EntityValue val;

        /** */
        @QuerySqlField
        private final int intVal1;

        /** */
        @QuerySqlField
        private final int intVal2;

        /** */
        public EntityValueValue(EntityValue val, int val1, int val2) {
            this.val = val;
            intVal1 = val1;
            intVal2 = val2;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "EVV[value=" + val + "]";
        }
    }
}
