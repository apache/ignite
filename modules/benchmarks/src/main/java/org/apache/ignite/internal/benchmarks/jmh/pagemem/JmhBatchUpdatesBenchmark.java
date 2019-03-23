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

package org.apache.ignite.internal.benchmarks.jmh.pagemem;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.logger.NullLogger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Batch updates in pagemem through preloader.
 *
 * todo benchmark for internal testing purposes.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xms3g", "-Xmx3g", "-server", "-XX:+AggressiveOpts", "-XX:MaxMetaspaceSize=256m"})
@Measurement(iterations = 11)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 15)
public class JmhBatchUpdatesBenchmark {
    /** */
    private static final long DEF_REG_SIZE = 3 * 1024 * 1024 * 1024L;

    /** */
    private static final int BATCH_SIZE = 500;

    /** */
    private static final String REG_BATCH = "batch-region";

    /** */
    private static final String REG_SINGLE = "single-region";

    /** */
    private static final String CACHE_BATCH = "batch";

    /** */
    private static final String CACHE_SINGLE = "single";

    /** */
    private static final String NODE_NAME = "srv0";

    /** */
    private static int iteration = 0;

    /** */
    public enum RANGE {
        /** */
        r0_4(0, 4),

        /** */
        r4_16(4, 16),

        /** */
        r16_64(16, 64),

        /** */
        r100_200(100, 200),

        /** */
        r200_500(200, 500),

        /** */
        r500_800(500, 800),

        /** */
        r800_1200(800, 1200),

        /** */
        r2000_3000(2_000, 3_000),

        /** */
        r1000_8000(1_000, 8_000),

        /** Large objects only. */
        r4000_16000(4_000, 16_000),

        /** Mixed objects, mostly large objects. */
        r100_32000(100, 32_000);

        /** */
        private final int min;

        /** */
        private final int max;

        /** */
        RANGE(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }


    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    private IgniteConfiguration getConfiguration(String cfgName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new NullLogger());

        cfg.setIgniteInstanceName(cfgName);

        DataRegionConfiguration reg1 = new DataRegionConfiguration();
        reg1.setInitialSize(DEF_REG_SIZE);
        reg1.setMaxSize(DEF_REG_SIZE);
        reg1.setName(REG_BATCH);

        DataRegionConfiguration reg2 = new DataRegionConfiguration();
        reg2.setInitialSize(DEF_REG_SIZE);
        reg2.setMaxSize(DEF_REG_SIZE);
        reg2.setName(REG_SINGLE);

        DataStorageConfiguration storeCfg = new DataStorageConfiguration();

        storeCfg.setDataRegionConfigurations(reg1, reg2);

        cfg.setDataStorageConfiguration(storeCfg);

        cfg.setCacheConfiguration(ccfg(false), ccfg(true));

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> ccfg(boolean batch) {
        return new CacheConfiguration<K, V>(batch ? CACHE_BATCH : CACHE_SINGLE)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setDataRegionName(batch ? REG_BATCH : REG_SINGLE);
    }

    /**
     * Test single updates.
     *
     * @param data Data that will be preloaded.
     * @param preloader Data preloader.
     */
    @Benchmark
    public void checkSingle(Data data, Preloader preloader) throws IgniteCheckedException {
        preloader.demanderSingle.preloadEntriesSingle(null, 0, data.singleData, data.cctxSingle.topology().readyTopologyVersion());
    }

    /**
     * Test batch updates.
     *
     * @param data Data that will be preloaded.
     * @param preloader Data preloader.
     */
    @Benchmark
    public void checkBatch(Data data, Preloader preloader) throws IgniteCheckedException {
        preloader.demanderBatch.preloadEntriesBatch(null, 0, data.batchData, data.cctxBatch.topology().readyTopologyVersion());
    }


    /**
     * Start 2 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        Ignition.start(getConfiguration(NODE_NAME));
    }

    /**
     * Stop all grids after tests.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * Create streamer on client cache.
     */
    @State(Scope.Benchmark)
    public static class Preloader {
        /** */
        final GridDhtPartitionDemander demanderBatch = demander(CACHE_BATCH);

        /** */
        final GridDhtPartitionDemander demanderSingle = demander(CACHE_SINGLE);

        /** */
        GridDhtPartitionDemander demander(String name) {
            GridCacheContext cctx = ((IgniteEx)Ignition.ignite(NODE_NAME)).cachex(name).context();

            GridDhtPreloader preloader = (GridDhtPreloader)cctx.group().preloader();

            return getFieldValue(preloader, "demander");
        }

        /**
         * Get object field value via reflection.
         *
         * @param obj Object or class to get field value from.
         * @param fieldNames Field names to get value for: obj->field1->field2->...->fieldN.
         * @param <T> Expected field class.
         * @return Field value.
         * @throws IgniteException In case of error.
         */
        public static <T> T getFieldValue(Object obj, String... fieldNames) throws IgniteException {
            assert obj != null;
            assert fieldNames != null;
            assert fieldNames.length >= 1;

            try {
                for (String fieldName : fieldNames) {
                    Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

                    try {
                        obj = findField(cls, obj, fieldName);
                    }
                    catch (NoSuchFieldException e) {
                        throw new RuntimeException(e);
                    }
                }

                return (T)obj;
            }
            catch (IllegalAccessException e) {
                throw new IgniteException("Failed to get object field [obj=" + obj +
                    ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
            }
        }

        /**
         * @param cls Class for searching.
         * @param obj Target object.
         * @param fieldName Field name for search.
         * @return Field from object if it was found.
         */
        private static Object findField(Class<?> cls, Object obj,
            String fieldName) throws NoSuchFieldException, IllegalAccessException {
            // Resolve inner field.
            Field field = cls.getDeclaredField(fieldName);

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            return field.get(obj);
        }
    }

    /**
     * Prepare and clean collection with streaming data.
     */
    @State(Scope.Thread)
    public static class Data {
        /** */
        @Param
        private RANGE range;

        /** */
        private int[] sizes;

        /** */
        Collection<GridCacheEntryInfo> batchData = new ArrayList<>(BATCH_SIZE);

        /** */
        Collection<GridCacheEntryInfo> singleData = new ArrayList<>(BATCH_SIZE);

        /** */
        GridCacheContext cctxBatch = ((IgniteEx)Ignition.ignite(NODE_NAME)).cachex(CACHE_BATCH).context();

        /** */
        GridCacheContext cctxSingle = ((IgniteEx)Ignition.ignite(NODE_NAME)).cachex(CACHE_SINGLE).context();

        /** */
        @Setup(Level.Trial)
        public void setup() {
            sizes = sizes(range.min, range.max, BATCH_SIZE);
        }

        /**
         * Prepare collection.
         */
        @Setup(Level.Iteration)
        public void prepare() {
            int iter = iteration++;

            int off = iter * BATCH_SIZE;

            batchData = prepareBatch(cctxBatch, off, BATCH_SIZE, sizes);
            singleData = prepareBatch(cctxSingle, off, BATCH_SIZE, sizes);
        }

        /**
         * Clean collection after each test.
         */
        @TearDown(Level.Iteration)
        public void cleanCollection() {
            batchData = null;
            singleData = null;
        }

        /** */
        int[] sizes(int minObjSize, int maxObjSize, int batchSize) {
            int sizes[] = new int[batchSize];
            int minSize = maxObjSize;
            int maxSize = minObjSize;

            int delta = maxObjSize - minObjSize;

            for (int i = 0; i < batchSize; i++) {
                int size = sizes[i] = minObjSize + (delta > 0 ? ThreadLocalRandom.current().nextInt(delta) : 0);

                if (size < minSize)
                    minSize = size;

                if (size > maxSize)
                    maxSize = size;
            }

            return sizes;
        }

        /**
         * Generates rebalance info objects.
         *
         * @param cctx Cache context.
         * @param off Offset.
         * @param cnt Count.
         * @param sizes Object sizes.
         * @return List of generated objects.
         */
        private List<GridCacheEntryInfo> prepareBatch(GridCacheContext cctx, int off, int cnt, int[] sizes) {
            List<GridCacheEntryInfo> infos = new ArrayList<>();

            for (int i = off; i < off + cnt; i++) {
                int size = sizes[i - off];

                KeyCacheObject key = cctx.toCacheKeyObject(i);
                CacheObject val = cctx.toCacheObject(new byte[size]);

                GridCacheEntryInfo info = new GridCacheEntryInfo();
                info.key(key);
                info.value(val);
                info.cacheId(cctx.cacheId());
                info.version(cctx.shared().versions().startVersion());

                infos.add(info);
            }

            return infos;
        }
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
            .include(JmhBatchUpdatesBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
