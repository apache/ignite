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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jol.info.GraphLayout;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class FileStoreHeapUtilizationJolBenchmark {
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

    /**
     * Cleans persistent directory.
     *
     * @throws Exception if failed.
     */
    private void cleanPersistenceDir() throws Exception {
        if (!F.isEmpty(G.allGrids()))
            throw new IgniteException("Grids are not stopped");

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /** */
    private IgniteConfiguration getConfiguration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryVmIpFinder()
                        .setAddresses(Collections.singleton("127.0.0.1:47500..47502"))
                )
        );

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

    /** */
    private Map<TestResultParameterInfo, Comparable> testGrid() {
        String name = UUID.randomUUID().toString().substring(0, 8);

        Ignite ignite = Ignition.start(getConfiguration(name));

        ignite.cluster().active(true);

        long start = System.currentTimeMillis();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, new byte[512]);

        for (int i = 50; i < 100; i++)
            cache.remove(i);

        for (int i = 50; i < 150; i++)
            cache.put(i, new byte[512]);

        long time = System.currentTimeMillis() - start;

        GraphLayout layout = GraphLayout.parseInstance(ignite);

        ignite.cluster().active(false);

        Ignition.stop(name, true);

        return new HashMap<TestResultParameterInfo, Comparable>() {{
            put(HEAP_USAGE_PARAM, layout.totalSize());
            put(CACHE_WORK_TIME_PARAM, time);
        }};
    }

    /** */
    private void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    private void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * Benchmark body
     *
     * @throws Exception if failed.
     */
    private void benchmark() throws Exception {
        beforeTest();

        Map<TestResultParameterInfo, Comparable> results = testGrid();

        afterTest();

        System.out.println("Benchmark results: ");

        results.forEach((k, v) -> System.out.println(k.name + ": " + v));
    }

    /** */
    public static void main(String[] args) throws Exception {
        new FileStoreHeapUtilizationJolBenchmark().benchmark();
    }

    /**
     * This class contains info about single parameter, which is measured by benchmark (e.g. heap usage, etc.).
     */
    private static class TestResultParameterInfo {
        /** */
        final String name;

        /** */
        final boolean greaterIsBetter;

        /** */
        TestResultParameterInfo(String name, boolean better) {
            this.name = name;
            greaterIsBetter = better;
        }
    }
}
