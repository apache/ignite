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

package org.apache.ignite.internal.benchmarks.jmh.disco;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**  * JMH Node failure detection timeout runner. */
@State(Scope.Benchmark)
public class JmhNodeFailureDetection extends JmhAbstractBenchmark {
    /** Enabled or disabled load on cluster with cache puts. */
    private static final boolean WITH_LOADING = true;

    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int ITERATIONS = 20;

    /** Node which gets failure simulation. */
    private Ignite illNode;

    /** */
    private volatile long beginTime;

    /** */
    private long endTime;

    /** */
    private final List<Long> results = new ArrayList<>();

    /** */
    private TcpDiscoverySpi specialSpi;

    /** */
    private final Object mux = new Object();

    /** */
    @TearDown
    public void stop() {
        Ignition.allGrids().forEach(this::silentStop);
    }

    /**
     * {@inheritDoc}
     */
    @Setup
    public void setup() throws Exception {
        Ignition.start(configuration("grid0"));

        Ignite node1 = Ignition.start(configuration("grid1"));

        if(WITH_LOADING)
            startLoad(node1);

        for (int i = 0; i < ITERATIONS; ++i) {
            beginTime = endTime = 0;

            // SPI of node 1 simulates node failure.
            specialSpi = illTcpDiscoSpi();

            illNode = Ignition.start(configuration("grid2"));

            illNode.events().localListen((e) -> {
                synchronized (mux) {
                    if (beginTime > 0 && endTime == 0) {
                        if (e.node().isLocal())
                            endTime = System.nanoTime();
                        else {
                            endTime = -1;

                            System.err.println("Wrong node failed: " + e.node().order() + ". Expected: " +
                                illNode.cluster().localNode().order());
                        }

                        mux.notifyAll();
                    }
                }

                return false;
            }, EventType.EVT_NODE_SEGMENTED, EventType.EVT_NODE_FAILED);

            specialSpi = null;

            // Randomizes node failure.
            Thread.sleep(new Random().nextInt(2000));

            synchronized (mux) {
                beginTime = System.nanoTime();

                mux.wait(10_000);

                if (endTime <= 0)
                    throw new IllegalStateException("Wrong node failed or not failed at all.");
                else {
                    long delay = U.nanosToMillis(endTime - beginTime);

                    beginTime = 0;

                    System.err.println("Detection delay: " + delay + ". Failure detection timeout: " +
                        illNode.configuration().getFailureDetectionTimeout() + ", connection recovery timeout: "
                        + ((TcpDiscoverySpi)illNode.configuration().getDiscoverySpi()).getConnectionRecoveryTimeout());

                    results.add(delay);
                }
            }

            silentStop(illNode);
        }

        System.out.println("Total detection delay: " + results.stream().mapToLong(v->v).sum());

        stop();
    }

    /** */
    @Benchmark
    public void measureTotalForTheOutput() throws InterruptedException {
        Thread.sleep(results.stream().mapToLong(v->v).sum());
    }

    /** */
    private TcpDiscoverySpi illTcpDiscoSpi() {
        return new TcpDiscoverySpi() {
            /** {@inheritDoc} */
            @Override protected void writeToSocket(Socket sock, OutputStream out,
                TcpDiscoveryAbstractMessage msg,
                long timeout) throws IOException, IgniteCheckedException {
                simulateSocketTimeout(timeout);

                super.writeToSocket(sock, out, msg, timeout);
            }

            /** {@inheritDoc} */
            @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
                long timeout) throws IOException {
                simulateSocketTimeout(timeout);

                super.writeToSocket(msg, sock, res, timeout);
            }

            /** {@inheritDoc} */
            @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
                long timeout) throws IOException {
                simulateSocketTimeout(timeout);

                super.writeToSocket(sock, msg, data, timeout);
            }

            /** */
            private void simulateSocketTimeout(long timeout) throws SocketTimeoutException {
                if (beginTime > 0) {
                    try {
                        Thread.sleep(timeout);

                        throw new SocketTimeoutException("Simulated timeout " + timeout + "ms.");
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }
                }
            }
        };
    }

    /**
     * Run benchmark.
     *
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .threads(1)
            .warmupIterations(0).measurementIterations(1)
            .benchmarks(JmhNodeFailureDetection.class.getSimpleName())
            .jvmArguments("-Xms512m", "-Xmx512m")
            .outputTimeUnit(TimeUnit.MINUTES)
            .run();
    }

    /** */
    protected IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        cfg.setFailureDetectionTimeout(1000);

        // Disable other message traffic.
        cfg.setMetricsUpdateFrequency(5_000);

        // Avoid useless warning. We do really block the threads.
        cfg.setSystemWorkerBlockedTimeout(10_000);

        TcpDiscoverySpi discoSpi = specialSpi == null ? new TcpDiscoverySpi() : specialSpi;
        discoSpi.setIpFinder(IP_FINDER);
        discoSpi.setLocalAddress("127.0.0.1");

        discoSpi.setConnectionRecoveryTimeout(500);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** */
    private void silentStop(Ignite ignite) {
        try {
            Ignition.stop(ignite.name(), true);
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /** */
    private void startLoad(Ignite node) {
        CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>("loadCache");
        cacheConfiguration.setCacheMode(CacheMode.REPLICATED);

        IgniteCache<Object, Object> cache = node.getOrCreateCache(cacheConfiguration);

        Stream.generate(() -> {
            return new Thread(() -> {
                Random rnd = new Random();

                System.out.println("Start loading.");

                while (true) {
                    try {
                        cache.put(rnd.nextInt(10_000), rnd.nextInt(10_000));
                    }
                    catch (Exception e) {
                        break;
                    }

//                    System.out.println("Put...");

                    try {
                        Thread.sleep(5);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }
                }

                System.out.println("Stop loading.");
            });
        }).limit(6).forEach(Thread::start);
    }
}
