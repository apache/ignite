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

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

/**
 * Tests for {@link JavaIgniteRDD} (embedded mode).
 */
public class IgniteRDDTypeMappingTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 1;

    /** Cache name. */
    private static final String CACHE_NAME = "test_cache";

    /** Integer to entity function. */
    private static final PairFunction<Integer, String, TestObj> INT_TO_TEST_OBJ_F =
        new PairFunction<Integer, String, TestObj>() {
            @Override public Tuple2<String, TestObj> call(Integer i) throws Exception {
                return new Tuple2<>(String.valueOf(i), new TestObj(i));
            }
        };

    /**
     * Default constructor.
     */
    public IgniteRDDTypeMappingTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Creates default spark context
     *
     * @return Context.
     */
    private JavaSparkContext createContext() {
        SparkConf conf = new SparkConf();

        conf.set("spark.executor.instances", String.valueOf(GRID_CNT));

        return new JavaSparkContext("local[" + GRID_CNT + "]", "test", conf);
    }


    /**
     * @throws Exception If failed.
     */
    public void testStructFieldQuery() throws Exception {
        JavaSparkContext sc = createContext();

        JavaIgniteContext<String, TestObj> ic = null;

        try {
            ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider(), false);

            JavaIgniteRDD<String, TestObj> cache = ic.fromCache(CACHE_NAME);

            cache.savePairs(sc.parallelize(F.range(0, 10), GRID_CNT).mapToPair(INT_TO_TEST_OBJ_F), true);

            DataFrame df = cache.sql("select intVal from TestObj where intVal = 1");
            df.printSchema();
            Row[] res = df.collect();
            assertEquals("Invalid result length", 1, res.length);
            assertEquals("Invalid result", 1, res[0].get(0));

            df = cache.sql("select structVal from TestObj where intVal = 1");
            df.printSchema();
            res = df.collect();
            assertEquals("Invalid result length", 1, res.length);
            assertEquals("Invalid result", TestObj.class, res[0].get(0).getClass());
            TestObj tmp = (TestObj)res[0].get(0);
            assertEquals("Invalid result", 1, tmp.structVal.intA);
            assertEquals("Invalid result", "str 1", tmp.structVal.strB);
        }
        finally {
            if (ic != null)
                ic.close(true);

            sc.stop();
        }
    }

    /** Finder. */
    private static TcpDiscoveryVmIpFinder FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * @param gridName Grid name.
     * @param client Client.
     * @throws Exception If failed.
     * @return Confiuration.
     */
    private static IgniteConfiguration getConfiguration(String gridName, boolean client) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setClientMode(client);

        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * Creates cache configuration.
     *
     * @return Cache configuration.
     */
    private static CacheConfiguration<Object, Object> cacheConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setBackups(1);

        ccfg.setName(CACHE_NAME);

        ccfg.setIndexedTypes(String.class, TestObj.class);

        return ccfg;
    }

    /**
     * Ignite configiration provider.
     */
    static class IgniteConfigProvider implements IgniteOutClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public IgniteConfiguration apply() {
            try {
                return getConfiguration("worker", false);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TestObj implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Int value. */
        @QuerySqlField(index = true)
        public int intVal;

        /** Struct value. */
        @QuerySqlField(index = true)
        public StructObj structVal;

        /**
         * @param intVal Int value.
         */
        public TestObj(int intVal) {
            this.intVal = intVal;
            structVal = new StructObj(intVal);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestObj obj = (TestObj)o;

            if (intVal != obj.intVal)
                return false;
            return structVal != null ? structVal.equals(obj.structVal) : obj.structVal == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = intVal;
            result = 31 * result + (structVal != null ? structVal.hashCode() : 0);
            return result;
        }
    }

    /**
     *
     */
    public static class StructObj implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Int a. */
        public int intA;

        /** String b. */
        public String strB;

        /**
         * @param intA Int a.
         */
        public StructObj(int intA) {
            this.intA = intA;
            strB = "str " + intA;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            StructObj obj = (StructObj)o;

            if (intA != obj.intA)
                return false;
            return strB != null ? strB.equals(obj.strB) : obj.strB == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = intA;
            result = 31 * result + (strB != null ? strB.hashCode() : 0);
            return result;
        }
    }
}
