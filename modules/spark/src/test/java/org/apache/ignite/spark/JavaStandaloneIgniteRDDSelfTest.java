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

package org.apache.ignite.spark;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;

/**
 * Tests for {@link JavaIgniteRDD} (standalone mode).
 */
public class JavaStandaloneIgniteRDDSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Keys count. */
    private static final int KEYS_CNT = 10000;

    /** Cache name. */
    private static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Sum function. */
    private static final Function2<Integer, Integer, Integer> SUM_F = new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    };

    /** To pair function. */
    private static final PairFunction<Integer, String, String> TO_PAIR_F = new PairFunction<Integer, String, String>() {
        /** {@inheritDoc} */
        @Override public Tuple2<String, String> call(Integer i) {
            return new Tuple2<>(String.valueOf(i), "val" + i);
        }
    };

    /** (String, Integer); pair to Integer value function. */
    private static final Function<Tuple2<String, Integer>, Integer> STR_INT_PAIR_TO_INT_F = new PairToValueFunction<>();

    /** (String, Entity) pair to Entity value function. */
    private static final Function<Tuple2<String, Entity>, Entity> STR_ENTITY_PAIR_TO_ENTITY_F =
        new PairToValueFunction<>();

    /** Integer to entity function. */
    private static final PairFunction<Integer, String, Entity> INT_TO_ENTITY_F =
        new PairFunction<Integer, String, Entity>() {
            @Override public Tuple2<String, Entity> call(Integer i) throws Exception {
                return new Tuple2<>(String.valueOf(i), new Entity(i, "name" + i, i * 100));
            }
        };

    /** */
    private static final PairFunction<Integer, String, EntityTestAllTypeFields> INT_TO_ENTITY_ALL_FIELDS_F =
        new PairFunction<Integer, String, EntityTestAllTypeFields>() {
            @Override public Tuple2<String, EntityTestAllTypeFields> call(Integer i) throws Exception {
                return new Tuple2<>(String.valueOf(i), new EntityTestAllTypeFields(i));
            }
        };

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignition.ignite("grid-0").cache(PARTITIONED_CACHE_NAME).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Ignition.stop("client", false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            Ignition.start(getConfiguration("grid-" + i, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            Ignition.stop("grid-" + i, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreDataToIgnite() throws Exception {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");

        try {
            JavaIgniteContext<String, String> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());

            ic.fromCache(PARTITIONED_CACHE_NAME)
                .savePairs(sc.parallelize(F.range(0, KEYS_CNT), 2).mapToPair(TO_PAIR_F));

            Ignite ignite = Ignition.ignite("grid-0");

            IgniteCache<String, String> cache = ignite.cache(PARTITIONED_CACHE_NAME);

            for (int i = 0; i < KEYS_CNT; i++) {
                String val = cache.get(String.valueOf(i));

                assertNotNull("Value was not put to cache for key: " + i, val);
                assertEquals("Invalid value stored for key: " + i, "val" + i, val);
            }
        }
        finally {
            sc.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadDataFromIgnite() throws Exception {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");

        try {
            JavaIgniteContext<String, Integer> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());

            Ignite ignite = Ignition.ignite("grid-0");

            IgniteCache<String, Integer> cache = ignite.cache(PARTITIONED_CACHE_NAME);

            for (int i = 0; i < KEYS_CNT; i++)
                cache.put(String.valueOf(i), i);

            JavaRDD<Integer> values = ic.fromCache(PARTITIONED_CACHE_NAME).map(STR_INT_PAIR_TO_INT_F);

            int sum = values.fold(0, SUM_F);

            int expSum = (KEYS_CNT * KEYS_CNT + KEYS_CNT) / 2 - KEYS_CNT;

            assertEquals(expSum, sum);
        }
        finally {
            sc.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryObjectsFromIgnite() throws Exception {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");

        try {
            JavaIgniteContext<String, Entity> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());

            JavaIgniteRDD<String, Entity> cache = ic.fromCache(PARTITIONED_CACHE_NAME);

            cache.savePairs(sc.parallelize(F.range(0, 1001), 2).mapToPair(INT_TO_ENTITY_F));

            List<Entity> res = cache.objectSql("Entity", "name = ? and salary = ?", "name50", 5000)
                .map(STR_ENTITY_PAIR_TO_ENTITY_F).collect();

            assertEquals("Invalid result length", 1, res.size());
            assertEquals("Invalid result", 50, res.get(0).id());
            assertEquals("Invalid result", "name50", res.get(0).name());
            assertEquals("Invalid result", 5000, res.get(0).salary());
            assertEquals("Invalid count", 500, cache.objectSql("Entity", "id > 500").count());
        }
        finally {
            sc.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryFieldsFromIgnite() throws Exception {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");

        try {
            JavaIgniteContext<String, Entity> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());

            JavaIgniteRDD<String, Entity> cache = ic.fromCache(PARTITIONED_CACHE_NAME);

            cache.savePairs(sc.parallelize(F.range(0, 1001), 2).mapToPair(INT_TO_ENTITY_F));

            DataFrame df =
                cache.sql("select id, name, salary from Entity where name = ? and salary = ?", "name50", 5000);

            df.printSchema();

            Row[] res = df.collect();

            assertEquals("Invalid result length", 1, res.length);
            assertEquals("Invalid result", 50, res[0].get(0));
            assertEquals("Invalid result", "name50", res[0].get(1));
            assertEquals("Invalid result", 5000, res[0].get(2));

            Column exp = new Column("NAME").equalTo("name50").and(new Column("SALARY").equalTo(5000));

            DataFrame df0 = cache.sql("select id, name, salary from Entity").where(exp);

            df.printSchema();

            Row[] res0 = df0.collect();

            assertEquals("Invalid result length", 1, res0.length);
            assertEquals("Invalid result", 50, res0[0].get(0));
            assertEquals("Invalid result", "name50", res0[0].get(1));
            assertEquals("Invalid result", 5000, res0[0].get(2));

            assertEquals("Invalid count", 500, cache.sql("select id from Entity where id > 500").count());
        }
        finally {
            sc.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllFieldsTypes() throws Exception {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "test");

        final int cnt = 100;

        try {
            JavaIgniteContext<String, EntityTestAllTypeFields> ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider());

            JavaIgniteRDD<String, EntityTestAllTypeFields> cache = ic.fromCache(PARTITIONED_CACHE_NAME);

            cache.savePairs(sc.parallelize(F.range(0, cnt), 2).mapToPair(INT_TO_ENTITY_ALL_FIELDS_F));

            EntityTestAllTypeFields e = new EntityTestAllTypeFields(cnt / 2);
            for(Field f : EntityTestAllTypeFields.class.getDeclaredFields()) {
                String fieldName = f.getName();

                Object val = GridTestUtils.getFieldValue(e, fieldName);

                DataFrame df = cache.sql(
                    String.format("select %s from EntityTestAllTypeFields where %s = ?", fieldName, fieldName),
                    val);

                if (val instanceof BigDecimal) {
                    Object res = df.collect()[0].get(0);

                    assertTrue(String.format("+++ Fail on %s field", fieldName),
                        ((Comparable<BigDecimal>)val).compareTo((BigDecimal)res) == 0);
                }
                else if (val instanceof java.sql.Date)
                    assertEquals(String.format("+++ Fail on %s field", fieldName),
                        val.toString(), df.collect()[0].get(0).toString());
                else if (val.getClass().isArray())
                    assertTrue(String.format("+++ Fail on %s field", fieldName), 1 <= df.count());
                else {
                    assertTrue(String.format("+++ Fail on %s field", fieldName), df.collect().length > 0);
                    assertTrue(String.format("+++ Fail on %s field", fieldName), df.collect()[0].size() > 0);
                    assertEquals(String.format("+++ Fail on %s field", fieldName), val, df.collect()[0].get(0));
                }

                info(String.format("+++ Query on the filed: %s : %s passed", fieldName, f.getType().getSimpleName()));
            }
        }
        finally {
            sc.stop();
        }
    }

    /**
     * @param gridName Grid name.
     * @param client Client.
     */
    private static IgniteConfiguration getConfiguration(String gridName, boolean client) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setClientMode(client);

        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * Creates cache configuration.
     */
    private static CacheConfiguration<Object, Object> cacheConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setBackups(1);

        ccfg.setName(PARTITIONED_CACHE_NAME);

        ccfg.setIndexedTypes(String.class, Entity.class,
            String.class, EntityTestAllTypeFields.class);

        return ccfg;
    }

    /**
     * Ignite configiration provider.
     */
    static class IgniteConfigProvider implements IgniteOutClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public IgniteConfiguration apply() {
            try {
                return getConfiguration("client", true);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @param <K>
     * @param <V>
     */
    static class PairToValueFunction<K, V> implements Function<Tuple2<K, V>, V> {
        /** {@inheritDoc} */
        @Override public V call(Tuple2<K, V> t) throws Exception {
            return t._2();
        }
    }
}
