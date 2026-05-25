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

package org.apache.ignite.internal.benchmarks.jmh.sql;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Abstract SQL queries benchmark.
 */
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
public abstract class JmhSqlAbstractBenchmark {
    /** Count of server nodes. */
    protected static final int SRV_NODES_CNT = 3;

    /** Keys count. */
    protected static final int KEYS_CNT = 100000;

    /** Size of batch. */
    protected static final int BATCH_SIZE = 1000;

    /** Partitions count. */
    protected static final int PARTS_CNT = 1024;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Query engine. */
    @Param({"H2", "CALCITE"})
    protected String engine;

    /** Ignite client. */
    protected Ignite client;

    /** Servers. */
    protected final Ignite[] servers = new Ignite[SRV_NODES_CNT];

    /** Cache. */
    private IgniteCache<Integer, Item> cache;

    /**
     * Create Ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Configuration.
     */
    protected IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);
        cfg.setLocalHost("127.0.0.1");
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
        cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
            "CALCITE".equals(engine) ? new CalciteQueryEngineConfiguration() : new IndexingQueryEngineConfiguration()
        ));

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Item> cacheConfiguration() {
        return new CacheConfiguration<Integer, Item>("CACHE")
            .setIndexedTypes(Integer.class, Item.class)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < SRV_NODES_CNT; i++)
            servers[i] = Ignition.start(configuration("server" + i));

        client = Ignition.start(configuration("client").setClientMode(true));

        cache = client.getOrCreateCache(cacheConfiguration());

        try (IgniteDataStreamer<Integer, Item> ds = client.dataStreamer("CACHE")) {
            for (int i = 0; i < KEYS_CNT; i++)
                ds.addData(i, new Item(i));
        }
    }

    /**
     * Stop Ignite instance.
     */
    @TearDown
    public void tearDown() {
        client.close();

        for (Ignite ignite : servers)
            ignite.close();
    }

    /** */
    protected List<List<?>> executeSql(String sql, Object... args) {
        return cache.query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }

    /** */
    protected static class Item {
        /** */
        @QuerySqlField
        private final String name;

        /** */
        @QuerySqlField
        private final int fld;

        /** */
        @QuerySqlField
        private final int fldBatch;

        /** */
        @QuerySqlField(index = true)
        private final int fldIdx;

        /** */
        @QuerySqlField(index = true)
        private final int fldIdxBatch;

        /** */
        public Item(int val) {
            name = "name" + val;
            fld = val;
            fldBatch = val / BATCH_SIZE;
            fldIdx = val;
            fldIdxBatch = val / BATCH_SIZE;
        }
    }
}
