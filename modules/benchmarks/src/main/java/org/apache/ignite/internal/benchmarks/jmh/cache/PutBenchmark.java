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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Put benchmark.
 */
@SuppressWarnings({"unchecked", "unused", "FieldCanBeLocal"})
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 120)
@Fork(1)
public class PutBenchmark {
    /** First Ignite instance. */
    private static Ignite ignite1;

    /** Second Ignite instance. */
    private static Ignite ignite2;

    /** Target cache. */
    private static IgniteCache<Integer, IntValue> cache1;

    /** Items count. */
    private static final int CNT = 100000;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * Set up routine.
     *
     * @throws Exception If failed.
     */
    @Setup
    public static void setup() throws Exception {
        ignite1 = Ignition.start(config("node1"));
        ignite2 = Ignition.start(config("node2"));

        cache1 = ignite1.cache(null);

        IgniteDataStreamer<Integer, IntValue> dataLdr = ignite1.dataStreamer(cache1.getName());

        for (int i = 0; i < CNT; i++)
            dataLdr.addData(i, new IntValue(i));

        dataLdr.close();

        System.out.println("Cache populated.");
    }

    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public static void tearDown() throws Exception {
        Ignition.stopAll(true);
    }

    /**
     * Create configuration.
     *
     * @param gridName Grid name.
     * @return Configuration.
     */
    private static IgniteConfiguration config(String gridName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(null);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Test PUT operation.
     *
     * @throws Exception If failed.
     */
    @Benchmark
    @Threads(4)
    public void testPut() throws Exception {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cache1.put(key, new IntValue(key));
    }

    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        // Use the following additional options to record JFR dump:
        // "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder", "-XX:StartFlightRecording=delay=20s,duration=60s,filename=ignite-put_bench.jfr"

        String[] jvmArgs = new String[] { "-Xms4g", "-Xmx4g" };

        Options opts = new OptionsBuilder().include(PutBenchmark.class.getSimpleName()).jvmArgs(jvmArgs).build();

        new Runner(opts).run();
    }
}
