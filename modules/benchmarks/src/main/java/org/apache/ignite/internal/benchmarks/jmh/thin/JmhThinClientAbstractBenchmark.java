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

package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.stream.IntStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Base class for thin client benchmarks.
 */
@State(Scope.Benchmark)
public abstract class JmhThinClientAbstractBenchmark extends JmhAbstractBenchmark {
    /** Property: nodes count. */
    protected static final String PROP_DATA_NODES = "ignite.jmh.thin.dataNodes";

    /** Default amount of nodes. */
    protected static final int DFLT_DATA_NODES = 4;

    /** Items count. */
    protected static final int CNT = 1000;

    /** Cache value. */
    protected static final byte[] PAYLOAD = new byte[1000];

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Default cache name. */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** Target node. */
    protected Ignite node;

    /** Target cache. */
    protected ClientCache<Integer, byte[]> cache;

    /** Thin client. */
    protected IgniteClient client;

    /**
     * Setup routine. Child classes must invoke this method first.
     *
     */
    @Setup
    public void setup() {
        System.out.println();
        System.out.println("--------------------");
        System.out.println("IGNITE BENCHMARK INFO: ");
        System.out.println("\tdata nodes:                 " + intProperty(PROP_DATA_NODES, DFLT_DATA_NODES));
        System.out.println("--------------------");
        System.out.println();

        int nodesCnt = intProperty(PROP_DATA_NODES, DFLT_DATA_NODES);

        A.ensure(nodesCnt >= 1, "nodesCnt >= 1");

        node = Ignition.start(configuration("node0"));

        for (int i = 1; i < nodesCnt; i++)
            Ignition.start(configuration("node" + i));

        String[] addrs = IntStream
                .range(10800, 10800 + nodesCnt)
                .mapToObj(p -> "127.0.0.1:" + p)
                .toArray(String[]::new);

        ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses(addrs)
                .setPartitionAwarenessEnabled(true);

        client = Ignition.startClient(cfg);

        cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        System.out.println("Loading test data...");

        for (int i = 0; i < CNT; i++)
            cache.put(i, PAYLOAD);

        System.out.println("Test data loaded: " + CNT);
    }

    /**
     * Tear down routine.
     *
     */
    @TearDown
    public void tearDown() throws Exception {
        client.close();
        Ignition.stopAll(true);
    }

    /**
     * Create Ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Configuration.
     */
    protected IgniteConfiguration configuration(String igniteInstanceName) {

        return new IgniteConfiguration()
                .setIgniteInstanceName(igniteInstanceName)
                .setLocalHost("127.0.0.1")
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
    }
}
