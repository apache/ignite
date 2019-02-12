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

package org.apache.ignite.internal.benchmarks.jmh.sequence;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import static org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner.createProperty;

/**
 * JMH benchmark for {@link IgniteAtomicSequence}.
 */
public class JmhSequenceBenchmark extends JmhAbstractBenchmark {

    /** Property: nodes count. */
    private static final String PROP_DATA_NODES = "ignite.jmh.sequence.dataNodes";

    /** Property: client mode flag. */
    private static final String PROP_CLIENT_MODE = "ignite.jmh.sequence.clientMode";

    /** Property: reservation batch size. */
    private static final String PROP_BATCH_SIZE = "ignite.jmh.sequence.batchSize";

    @State(Scope.Benchmark)
    public static class SequenceState {
        /** IP finder shared across nodes. */
        private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

        /** 1/10 of batchSize. */
        private int randomBound;

        /** */
        private IgniteAtomicSequence seq;

        /**
         * Setup.
         */
        @Setup
        public void setup() {
            Ignite node = Ignition.start(configuration("NODE_0"));

            int nodes = intProperty(PROP_DATA_NODES, 4);

            for (int i = 1; i < nodes; i++)
                Ignition.start(configuration("NODE_" + i));

            boolean isClient = booleanProperty(PROP_CLIENT_MODE);

            if (isClient) {
                IgniteConfiguration clientCfg = configuration("client");

                clientCfg.setClientMode(true);

                node = Ignition.start(clientCfg);
            }

            AtomicConfiguration acfg = new AtomicConfiguration();

            int batchSize = intProperty(PROP_BATCH_SIZE);

            randomBound = batchSize < 10 ? 1 : batchSize / 10;

            acfg.setAtomicSequenceReserveSize(batchSize);

            seq = node.atomicSequence("seq", acfg, 0, true);
        }

        /**
         * Create Ignite configuration.
         *
         * @param igniteInstanceName Ignite instance name.
         * @return Configuration.
         */
        private IgniteConfiguration configuration(String igniteInstanceName) {
            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setIgniteInstanceName(igniteInstanceName);

            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(discoSpi);

            return cfg;
        }

        /**
         * Tear down routine.
         */
        @TearDown
        public void tearDown() {
            Ignition.stopAll(true);
        }
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(false, 4, 10_000);
        run(false, 4, 100_000);

        run(true, 4, 10_000);
        run(true, 4, 100_000);
    }

    /**
     * @param client Client node.
     * @param dataNodes Number of data nodes.
     * @param batchSize Batch size.
     * @throws Exception If failed.
     */
    private static void run(boolean client, int dataNodes, int batchSize) throws Exception {
        String simpleClsName = JmhSequenceBenchmark.class.getSimpleName();

        String output = simpleClsName +
            "-" + (client ? "client" : "data") +
            "-" + dataNodes +
            "-" + batchSize;

        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(5)
            .warmupIterations(10)
            .measurementIterations(20)
            .output(output + ".jmh.log")
            .benchmarks(JmhSequenceBenchmark.class.getSimpleName())
            .jvmArguments(
                "-Xms4g",
                "-Xmx4g",
                createProperty(PROP_BATCH_SIZE, batchSize),
                createProperty(PROP_DATA_NODES, dataNodes),
                createProperty(PROP_CLIENT_MODE, client)
            )
            .run();
    }

    /**
     * Benchmark for {@link IgniteAtomicSequence#incrementAndGet} operation.
     *
     * @return Long new value.
     */
    @Benchmark
    public long incrementAndGet(SequenceState state) {
        return state.seq.incrementAndGet();
    }

    /**
     * Benchmark for {@link IgniteAtomicSequence#getAndIncrement()} operation.
     *
     * @return Long previous value.
     */
    @Benchmark
    public long getAndIncrement(SequenceState state) {
        return state.seq.getAndIncrement();
    }

    /**
     * Benchmark for {@link IgniteAtomicSequence#addAndGet(long)} operation.
     *
     * @return Long new value.
     */
    @Benchmark
    public long addAndGet(SequenceState state) {
        int key = ThreadLocalRandom.current().nextInt(state.randomBound) + 1;

        return state.seq.getAndAdd(key);
    }

    /**
     * Benchmark for {@link IgniteAtomicSequence#getAndAdd(long)} operation.
     *
     * @return Long previous value.
     */
    @Benchmark
    public long getAndAdd(SequenceState state) {
        int key = ThreadLocalRandom.current().nextInt(state.randomBound) + 1;

        return state.seq.getAndAdd(key);
    }
}
