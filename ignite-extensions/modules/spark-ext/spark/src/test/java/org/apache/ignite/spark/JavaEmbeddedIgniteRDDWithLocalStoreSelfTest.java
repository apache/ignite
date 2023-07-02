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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import scala.Tuple2;

/**
 * Tests for {@link JavaIgniteRDD} (embedded mode).
 */
public class JavaEmbeddedIgniteRDDWithLocalStoreSelfTest extends GridCommonAbstractTest {
    /** */
    private static ConcurrentHashMap<Object, Object> storeMap;

    /** */
    private TestStore store;

    /** For Ignite instance names generation */
    private static AtomicInteger cntr = new AtomicInteger(1);

    /** Ignite instance names. */
    private static ThreadLocal<Integer> igniteInstanceNames = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return cntr.getAndIncrement();
        }
    };

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Cache name. */
    private static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** To pair function. */
    private static final PairFunction<Integer, Integer, Integer> SIMPLE_FUNCTION = new PairFunction<Integer, Integer, Integer>() {
        /** {@inheritDoc} */
        @Override public Tuple2<Integer, Integer> call(Integer i) {
            return new Tuple2<>(i, i);
        }
    };

    /**
     * Default constructor.
     */
    public JavaEmbeddedIgniteRDDWithLocalStoreSelfTest() {
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
    @Test
    public void testStoreDataToIgniteWithOptionSkipStore() throws Exception {
        storeMap = new ConcurrentHashMap<>();
        store = new TestStore();

        JavaSparkContext sc = createContext();

        JavaIgniteContext<Integer, Integer> ic = null;

        try {
            ic = new JavaIgniteContext<>(sc, new IgniteConfigProvider(), false);

            Ignite ignite = ic.ignite();

            IgniteCache<Integer, Integer> cache = ignite.cache(PARTITIONED_CACHE_NAME);

            for (int i = 0; i < 1000; i++)
                storeMap.put(i, i);

            ic.fromCache(PARTITIONED_CACHE_NAME)
                .savePairs(sc.parallelize(F.range(1000, 2000), GRID_CNT).mapToPair(SIMPLE_FUNCTION), true, false);

            for (int i = 0; i < 2000; i++)
                assertEquals(i, storeMap.get(i));

            ic.fromCache(PARTITIONED_CACHE_NAME)
                .savePairs(sc.parallelize(F.range(2000, 3000), GRID_CNT).mapToPair(SIMPLE_FUNCTION), true, true);

            for (int i = 2000; i < 3000; i++)
                assertNull(storeMap.get(i));

            for (int i = 0; i < 3000; i++) {
                Integer val = cache.get(i);

                assertNotNull("Value was not put to cache for key: " + i, val);
                assertEquals("Invalid value stored for key: " + i, Integer.valueOf(i), val);
            }
        }
        finally {
            if (ic != null)
                ic.close(true);

            sc.stop();
        }
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param client Client.
     * @throws Exception If failed.
     * @return Confiuration.
     */
    private static IgniteConfiguration getConfiguration(String igniteInstanceName, boolean client) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setClientMode(client);

        cfg.setIgniteInstanceName(igniteInstanceName);

        return cfg;
    }

    /**
     * Creates cache configuration.
     *
     * @return Cache configuration.
     */
    private static CacheConfiguration<Object, Object> cacheConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setBackups(1);

        ccfg.setName(PARTITIONED_CACHE_NAME);

        ccfg.setIndexedTypes(String.class, Entity.class);

        ccfg.setCacheStoreFactory(FactoryBuilder.factoryOf(TestStore.class));

        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);

        return ccfg;
    }

    /**
     * Ignite configiration provider.
     */
    static class IgniteConfigProvider implements IgniteOutClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public IgniteConfiguration apply() {
            try {
                return getConfiguration("worker-" + igniteInstanceNames.get(), false);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }
    }
}
