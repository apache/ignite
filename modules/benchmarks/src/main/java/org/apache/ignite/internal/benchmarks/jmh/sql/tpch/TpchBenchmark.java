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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Benchmark simple SQL queries.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Timeout(time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 3, time = 10)
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
//        throw new RuntimeException("Provide path to directory containing <table_name>.tbl files");
//        return "/home/skor/work/ISE-5583-Calcite-vs-H2/dataset/0.0001";
        return "/home/skor/work/ISE-5583-Calcite-vs-H2/dataset/0.1";
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
    public void q1(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(1));
    }

    /** Q2. */
    @Benchmark
    public void q2(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(2));
    }

    /** Q3. */
    @Benchmark
    public void q3(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(3));
    }

    /** Q4. */
    @Benchmark
    public void q4(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(4));
    }

    /** Q5. */
    @Benchmark
    public void q5(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(5));
    }

    /** Q6. */
    @Benchmark
    public void q6(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(6));
    }

    /** Q7. */
    @Benchmark
    public void q7(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(7));
    }

    /** Q8. */
    @Benchmark
    public void q8(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(8));
    }

    /** Q9. */
    @Benchmark
    public void q9(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(9));
    }

    /** Q10. */
    @Benchmark
    public void q10(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(10));
    }

    /** Q11. */
    @Benchmark
    public void q11(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(11));
    }

    /** Q12. */
    @Benchmark
    public void q12(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(12));
    }

    /** Q13. */
    @Benchmark
    public void q13(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(13));
    }

    /** Q14. */
    @Benchmark
    public void q14(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(14));
    }

    /** Q15. */
    @Benchmark
    public void q15(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(15));
    }

    /** Q16. */
    @Benchmark
    public void q16(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(16));
    }

    /** Q17. */
    @Benchmark
    public void q17(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(17));
    }

    /** Q18. */
    @Benchmark
    public void q18(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(18));
    }

    /** Q19. */
    @Benchmark
    public void q19(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(19));
    }

    /** Q20. */
    @Benchmark
    public void q20(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(20));
    }

    /** Q21. */
    @Benchmark
    public void q21(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(21));
    }

    /** Q22. */
    @Benchmark
    public void q22(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(22));
    }

    /** */
    private void executeSql(Blackhole bh, String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setTimeout(60, TimeUnit.SECONDS);

        CompletableFuture<Void> future = runAsync(() -> {
            FieldsQueryCursor<List<?>> cursor = ((IgniteEx) client).context().query().querySqlFields(qry, false);

            cursor.forEach(bh::consume);
        });

        try {
            future.get(60, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            bh.consume(e);
        }
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
