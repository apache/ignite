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
 *
 */

package org.apache.ignite.compatibility.persistence;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
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
 * Checks all basic sql types work correctly.
 */
@RunWith(Parameterized.class)
public class IndexTypesCompatibilityTest extends IndexAbstractCompatibilityTest {
    /** */
    private static final String TEST_CACHE_NAME = IndexTypesCompatibilityTest.class.getSimpleName();

    /** */
    private static final int ROWS_CNT = 100;

    /** */
    private static final String TABLE_PREFIX = "TABLE_";

    /**
     * Key is one of valid Ignite SQL types.
     * Value is list of functions that produces values with a Java type related to SQL type.
     * First function is for direct type matching, others are synonims.
     */
    private static final Map<String, List<Function<Integer, Object>>> typeProducer = new HashMap<>();

    static {
        register("Boolean", (i) -> i > 0, Integer::new);
        register("Tinyint", Integer::byteValue, Integer::shortValue, Integer::new, Long::new);
        register("Bigint", Integer::longValue, Integer::new);
        register("Decimal", BigDecimal::new);
        register("Double", Integer::doubleValue, Float::new);
        register("Int", Integer::new);
        register("Real", Integer::floatValue, Integer::doubleValue);
        register("Smallint", Integer::shortValue, Integer::new);
        register("Char", (i) -> String.format("%4s", i.toString())); // Padding string for correct comparison.
        register("Varchar", (i) -> String.format("%4s", i.toString()));
        register("Date", (i) -> new java.sql.Date(i, Calendar.JANUARY, 1)); // LocalDate is not interchangeable.
        register("Time", Time::new);  // Time and LocalTime are not interchanheable as differently implement hashCode.
        register("Timestamp",  // Instant is not supported, LocalDateTime calculates hashCode differently.
            Timestamp::new, (i) -> java.util.Date.from(Instant.ofEpochMilli(i)));
        register("Binary", (i) -> ByteBuffer.allocate(4).putInt(i).array());
        register("UUID", (i) -> UUID.fromString("123e4567-e89b-12d3-a456-" +
            String.format("%12s", i.toString()).replace(' ', '0')));
    }

    /** */
    private static void register(String type, Function<Integer, Object>... funcs) {
        typeProducer.put(type, Arrays.asList(funcs));
    }

    /** Parametrized run param: Ignite version. */
    @Parameterized.Parameter
    public String igniteVer;

    /** Test run configurations: Ignite version, Inline size configuration. */
    @Parameterized.Parameters(name = "ver={0}")
    public static Collection<Object[]> runConfig() {
        return cartesianProduct(since(VER_2_6_0));
    }

    /** */
    @Test
    public void testQueryOldIndex() throws Exception {
        int majorJavaVer = U.majorJavaVersion(U.jdkVersion());

        if (majorJavaVer > 11) {
            Assume.assumeTrue("Skipped on jdk " + U.jdkVersion(),
                    VER_2_12_0.compareTo(IgniteReleasedVersion.fromString(igniteVer)) < 0);
        }

        doTestStartupWithOldVersion(igniteVer, new PostStartupClosure());
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    protected void doTestStartupWithOldVersion(String igniteVer, PostStartupClosure closure) throws Exception {
        try {
            startGrid(1, igniteVer,
                new PersistenceBasicCompatibilityTest.ConfigurationClosure(true),
                closure);

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.cluster().state(ClusterState.ACTIVE);

            validateResultingCacheData(ignite.cache(TEST_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Asserts cache contained all expected values as it was saved before.
     *
     * @param cache Cache to check.
     */
    private void validateResultingCacheData(IgniteCache<Object, Object> cache) {
        for (String type: typeProducer.keySet()) {
            // Byte array is supported as key since 2.8.0.
            if ("Binary".equals(type) && igniteVer.compareTo("2.8.0") < 0)
                continue;

            // In versions prior to 2.7.0, UUID must be converted to byte array to work with.
            if ("UUID".equals(type) && igniteVer.compareTo("2.7.0") < 0)
                continue;

            int cnt = getRowsCnt(type);

            for (Function<Integer, Object> func: typeProducer.get(type)) {
                for (int i = 0; i < cnt; i++) {
                    validateRandomRow(cache, type, func, i);
                    validateRandomRange(cache, type, func, i);
                }

                validateNull(cache, type, func);
            }
        }
    }

    /** */
    private void validateRandomRow(IgniteCache<Object, Object> cache, String type, Function<Integer, Object> func, int inc) {
        String table = TABLE_PREFIX + type;

        Object val = func.apply(inc);

        // Select by quering complex index.
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM public." + table + " v WHERE id = ?;")
            .setArgs(val);

        checkIndexUsed(cache, qry, "_key_PK");

        List<List<?>> result = cache.query(qry).getAll();

        assertTrue("Type=" + type + "; inc=" + inc + "; size=" + result.size(), result.size() == 1);

        List<?> row = result.get(0);

        for (int i = 0; i < 2; i++) {
            if ("Binary".equals(type))
                assertTrue("Type=" + type + "; exp=" + val + "; act=" + row.get(i),
                    Arrays.equals((byte[])val, (byte[])row.get(i)));
            else
                assertTrue("Type=" + type + "; exp=" + val + "; act=" + row.get(i), row.get(i).equals(getBaseValue(type, inc)));
        }
    }

    /** */
    private void validateNull(IgniteCache<Object, Object> cache, String type, Function<Integer, Object> func) {
        if ("Date".equals(type) || "Boolean".equals(type) || "Tinyint".equals(type))
            return;

        String table = TABLE_PREFIX + type;

        int inc = getRowsCnt(type);
        Object val = func.apply(inc);

        // Select by quering complex index.
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM public." + table + " v WHERE id = ?;")
            .setArgs(val);

        checkIndexUsed(cache, qry, "_key_PK");

        List<List<?>> result = cache.query(qry).getAll();

        assertTrue("Type=" + type + "; inc=" + inc + "; size=" + result.size(), result.size() == 1);

        List<?> row = result.get(0);

        assertNull("Type=" + type, row.get(1));
    }

    /** */
    private void validateRandomRange(IgniteCache<Object, Object> cache, String type, Function<Integer, Object> func, int pivot) {
        String table = TABLE_PREFIX + type;

        int cnt = getRowsCnt(type);

        Object val = func.apply(pivot);

        // Select by quering complex index.
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "SELECT * FROM public." + table + " v WHERE id > ? AND val is not null ORDER BY id;").setArgs(val);

        checkIndexUsed(cache, qry, "_key_PK");

        List<List<?>> result = cache.query(qry).getAll();

        // For strict comparison. There was an issues with >= comparison for some versions.
        pivot += 1;

        assertTrue("Type= " + type + "; exp=" + (cnt - pivot) + "; act=" + result.size(), result.size() == cnt - pivot);

        for (int i = 0; i < cnt - pivot; i++) {
            List<?> row = result.get(i);

            val = getBaseValue(type, pivot + i);

            if ("Binary".equals(type))
                assertTrue("Type=" + type + "; exp=" + val + "; act=" + row.get(0),
                    Arrays.equals((byte[])val, (byte[])row.get(0)));
            else
                assertTrue("Type=" + type + "; exp=" + val + "; act=" + row.get(0),
                    row.get(0).equals(val));
        }
    }

    /**
     * @return Count of test rows for every type. Boolean and Tinyint are limited by definition.
     */
    private static int getRowsCnt(String type) {
        if ("Boolean".equals(type))
            return 2;
        else if ("Tinyint".equals(type))
            return 128;
        else
            return ROWS_CNT;
    }

    /**
     * @return Value produced by function of direct matching of Ignite type to Java class.
     */
    private static Object getBaseValue(String type, int val) {
        return typeProducer.get(type).get(0).apply(val);
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

            IgniteCache<Object, Object> cache = ignite.createCache(cacheCfg);

            saveCacheData(cache);

            ignite.active(false);
        }

        /**
         * Create a complex index (int, pojo, int). Check that middle POJO object is correctly available from inline.
         */
        protected void saveCacheData(IgniteCache<Object, Object> cache) {
            // WRAP_VALUE is a hack to make Date column work as PK.
            String createQry = "CREATE TABLE public.%s (id %s PRIMARY KEY, val %s)";

            String insertQry = "INSERT INTO public.%s (id, val) values (?, ?)";

            for (String type: typeProducer.keySet()) {
                String table = "TABLE_" + type;

                String create = String.format(createQry, table, type, type);
                String insert = String.format(insertQry, table);

                // https://issues.apache.org/jira/browse/IGNITE-8552
                // Date can't be used as PK column without this setting.
                if ("Date".equals(type))
                    create += " with \"WRAP_VALUE=false\"";

                cache.query(new SqlFieldsQuery(create));

                for (int i = 0; i < getRowsCnt(type); i++) {
                    Object val = getBaseValue(type, i);

                    cache.query(new SqlFieldsQuery(insert).setArgs(val, val)).getAll();
                }

                // Put NULL with last insert. Skip the check for types with limited size (Boolean, Tinyint)
                // and for Date as it can be created only with WRAP_VALUE=false setting and it does not allow nulls.
                if (!"Date".equals(type) && !"Boolean".equals(type) && !"Tinyint".equals(type)) {
                    Object val = getBaseValue(type, getRowsCnt(type));

                    cache.query(new SqlFieldsQuery(insert).setArgs(val, null)).getAll();
                }
            }
        }
    }

    /**
     * Too many insert queries for every type. Need wait more time.
     */
    @Override protected long getNodeJoinTimeout() {
        return 60_000;
    }
}
