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

package org.apache.ignite.internal.benchmarks.jmh.sql.tpch;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchHelper;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark simple SQL queries.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Timeout(time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class TpchBenchmark {
    /** Count of server nodes. */
    private static final int SRV_NODES_CNT = 3;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Query engine. */
    @Param({"CALCITE"})
    private String engine;

    /** Ignite client. */
    private Ignite client;

    /** Servers. */
    private final Ignite[] servers = new Ignite[SRV_NODES_CNT];

    /** */
    String pathToDataset() {
        throw new RuntimeException("Provide path to directory containing <table_name>.tbl files");
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
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
        cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
            "CALCITE".equals(engine) ? new CalciteQueryEngineConfiguration() : new IndexingQueryEngineConfiguration()
        ));

        return cfg;
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < SRV_NODES_CNT; i++)
            servers[i] = Ignition.start(configuration("server" + i));

        client = Ignition.start(configuration("client").setClientMode(true));

        TpchHelper.createTables(client);

        TpchHelper.fillTables(client, pathToDataset());
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

    /** Q1. */
    @Benchmark
    public void q1() {
        executeSql(TpchHelper.getQuery(1));
    }

    /** Q2. */
    @Benchmark
    public void q2() {
        executeSql(TpchHelper.getQuery(2));
    }

    /** Q3. */
    @Benchmark
    public void q3() {
        executeSql(TpchHelper.getQuery(3));
    }

    /** Q4. */
    @Benchmark
    public void q4() {
        executeSql(TpchHelper.getQuery(4));
    }

    /** Q5. */
    @Benchmark
    public void q5() {
        executeSql(TpchHelper.getQuery(5));
    }

    /** Q6. */
    @Benchmark
    public void q6() {
        executeSql(TpchHelper.getQuery(6));
    }

    /** Q7. */
    @Benchmark
    public void q7() {
        executeSql(TpchHelper.getQuery(7));
    }

    /** Q8. */
    @Benchmark
    public void q8() {
        executeSql(TpchHelper.getQuery(8));
    }

    /** Q9. */
    @Benchmark
    public void q9() {
        executeSql(TpchHelper.getQuery(9));
    }

    /** Q10. */
    @Benchmark
    public void q10() {
        executeSql(TpchHelper.getQuery(10));
    }

    /** Q11. */
    @Benchmark
    public void q11() {
        executeSql(TpchHelper.getQuery(11));
    }

    /** Q12. */
    @Benchmark
    public void q12() {
        executeSql(TpchHelper.getQuery(12));
    }

    /** Q13. */
    @Benchmark
    public void q13() {
        executeSql(TpchHelper.getQuery(13));
    }

    /** Q14. */
    @Benchmark
    public void q14() {
        executeSql(TpchHelper.getQuery(14));
    }

    /** Q15. */
    @Benchmark
    public void q15() {
        executeSql(TpchHelper.getQuery(15));
    }

    /** Q16. */
    @Benchmark
    public void q16() {
        executeSql(TpchHelper.getQuery(16));
    }

    /** Q17. */
    @Benchmark
    public void q17() {
        executeSql(TpchHelper.getQuery(17));
    }

    /** Q18. */
    @Benchmark
    public void q18() {
        executeSql(TpchHelper.getQuery(18));
    }

    /** Q19. */
    @Benchmark
    public void q19() {
        executeSql(TpchHelper.getQuery(19));
    }

    /** Q20. */
    @Benchmark
    public void q20() {
        executeSql(TpchHelper.getQuery(20));
    }

    /** Q21. */
    @Benchmark
    public void q21() {
        executeSql(TpchHelper.getQuery(21));
    }

    /** Q22. */
    @Benchmark
    public void q22() {
        executeSql(TpchHelper.getQuery(22));
    }

    /** */
    private List<List<?>> executeSql(String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setTimeout(10, TimeUnit.SECONDS);

        return ((IgniteEx)client).context().query().querySqlFields(qry, false).getAll();
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(TpchBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
