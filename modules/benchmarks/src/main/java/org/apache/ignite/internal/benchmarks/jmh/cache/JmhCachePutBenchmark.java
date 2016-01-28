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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Put benchmark.
 */
@SuppressWarnings({"unchecked", "unused", "FieldCanBeLocal"})
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 120)
@Fork(1)
public class JmhCachePutBenchmark extends JmhCacheAbstractBenchmark {
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

    public void setup() throws Exception {
        super.setup();

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
     * Create configuration.
     *
     * @param gridName Grid name.
     * @return Configuration.
     */
    private IgniteConfiguration config(String gridName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

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
        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .warmupIterations(10)
            .measurementIterations(2000)
            .classes(JmhCachePutBenchmark.class)
            .jvmArguments(
                "-Xms4g",
                "-Xmx4g",
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=delay=20s,duration=60s,dumponexit=true,settings=alloc,filename=ignite-put_bench.jfr",
                JmhIdeBenchmarkRunner.createProperty(PROP_BACKUPS, 1),
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, CacheAtomicityMode.ATOMIC),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.PRIMARY_SYNC))
            .run();
    }
}
