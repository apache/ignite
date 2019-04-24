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
package org.apache.ignite.internal.benchmarks.jol;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.benchmarks.AbstractOptimizationTestBenchmark;
import org.openjdk.jol.info.GraphLayout;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_FILE_STORE_HEAP_OPTIMIZATION;

/**
 *
 */
public class FileStoreHeapUtilizationJolBenchmark extends AbstractOptimizationTestBenchmark {
    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private static final String HEAP_USAGE = "heap usage";

    /** */
    private static final String CACHE_WORK_TIME = "cache work time";

    /** */
    private static final TestResultParameterInfo HEAP_USAGE_PARAM = new TestResultParameterInfo(HEAP_USAGE, false);

    /** */
    private static final TestResultParameterInfo CACHE_WORK_TIME_PARAM = new TestResultParameterInfo(CACHE_WORK_TIME, false);

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, CacheConfiguration.MAX_PARTITIONS_COUNT));

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void enableOptimization(boolean enable) {
        System.setProperty(IGNITE_ENABLE_FILE_STORE_HEAP_OPTIMIZATION, String.valueOf(enable));
    }

    /** {@inheritDoc} */
    @Override protected Map<TestResultParameterInfo, Comparable> testGrid() {
        String name = UUID.randomUUID().toString().substring(0, 8);

        Ignite ignite = Ignition.start(getConfiguration(name));

        ignite.cluster().active(true);

        GraphLayout layout = GraphLayout.parseInstance(ignite);

        long start = System.currentTimeMillis();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, new byte[512]);

        for (int i = 50; i < 100; i++)
            cache.remove(i);

        for (int i = 50; i < 150; i++)
            cache.put(i, new byte[512]);

        long time = System.currentTimeMillis() - start;

        ignite.cluster().active(false);

        Ignition.stop(name, true);

        return new HashMap<TestResultParameterInfo, Comparable>() {{
            put(HEAP_USAGE_PARAM, layout.totalSize());
            put(CACHE_WORK_TIME_PARAM, time);
        }};
    }

    /** {@inheritDoc} */
    @Override protected Collection<TestResultParameterInfo> testingResults() {
        return Arrays.asList(HEAP_USAGE_PARAM, CACHE_WORK_TIME_PARAM);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeBenchmark() {
        //warming up
        testGrid();
    }

    /** */
    public static void main(String[] args) throws Exception {
        new FileStoreHeapUtilizationJolBenchmark().benchmark();
    }
}
