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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark TPC-H SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
@Threads(1)
@OutputTimeUnit(TimeUnit.SECONDS)
public class TpchBenchmark {
    /*
        By default, this benchmark creates a separate work directory for each scale factor value.
        TPC-H dataset of the corresponding scale is generated in the `tpch_dataset` subdirectory.
        Don't forget to remove the directory after benchmark is finished.

        If persistence is used (it's so by default) dataset is loaded into the ignite cluster only
        once to speed up testing. However, cluster is restarted before each benchmark run.
    */

    /** Count of server nodes. */
    private static final int SRV_NODES_CNT = 3;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Path to the dataset. */
    private Path datasetPath;

    /** If true the dataset will be loaded only once to speed up the testing. */
    private static final Boolean PERSISTENT = true;

    /** */
    private static final String DATASET_READY_MARK_FILE_NAME = "ready.txt";

    /** Query engine. */
    @Param({"CALCITE", "H2"})
    private String engine;

    /** Scale factor. "1" means about 1Gb of data. */
    @Param({"0.01", "0.1", "1"})
    private String scale;

    /**
     * Query id.
     * The commented queries do not currently work for Calcite.
     * The 11, 13, 15, 19 queries also do not work for H2.
     */
    @Param({
        "1", /*"2",*/ "3", "4", /*"5",*/
        "6", "7", /*"8",*/ /*"9",*/ "10",
        "11", "12", "13", "14", /*"15",*/
        /*"16",*/ /*"17",*/ "18", /*"19",*/ /*"20",*/
        /*"21",*/ "22"})
    private String queryId;

    /** Ignite client. */
    private Ignite client;

    /** Servers. */
    private final Ignite[] servers = new Ignite[SRV_NODES_CNT];

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

        if (PERSISTENT) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)));
        }

        cfg.setWorkDirectory(Path.of(U.getIgniteHome(), String.format("work-%s", scale)).toString());

        return cfg;
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() throws IOException, IgniteCheckedException {
        for (int i = 0; i < SRV_NODES_CNT; i++)
            servers[i] = Ignition.start(configuration("server" + i));

        servers[0].cluster().state(ClusterState.ACTIVE);

        client = Ignition.start(configuration("client").setClientMode(true));

        fillData();
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

    /**
     * Test already planned and cached query (without the initial planning).
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 3, time = 10)
    public void cached(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(Integer.parseInt(queryId)));
    }

    /**
     * Test a single cold non-cached query (include initial planning).
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 1)
    public void cold(Blackhole bh) {
        executeSql(bh, TpchHelper.getQuery(Integer.parseInt(queryId)));
    }

    /** */
    private void executeSql(Blackhole bh, String sql) {
        try {
            for (String q : sql.split(";")) {
                if (!q.trim().isEmpty()) {
                    SqlFieldsQuery qry = new SqlFieldsQuery(q.trim()).setTimeout(60, TimeUnit.SECONDS);

                    FieldsQueryCursor<List<?>> cursor = ((IgniteEx)client).context().query().querySqlFields(qry, false);

                    cursor.forEach(bh::consume);
                }
            }
        }
        catch (Exception e) {
            tearDown();

            throw e;
        }

    }

    /**
     * Generate TPC-H dataset, create and fill tables.
     * <p>
     * The dataset .tbl files are created only once in subdirectory in the Ignite work dir.
     * Subsequent runs will use previously generated dataset.
     * <p>
     * If persistent storage is used, then the dataset will be loaded only once.
     */
    private void fillData() throws IOException, IgniteCheckedException {
        if (datasetPath == null)
            datasetPath = U.resolveWorkDirectory(client.configuration().getWorkDirectory(),"tpch_dataset", false).toPath();

        if (!Files.exists(datasetPath.resolve(DATASET_READY_MARK_FILE_NAME))) {
            TpchHelper.generateData(Double.parseDouble(scale), datasetPath);

            Files.createFile(datasetPath.resolve(DATASET_READY_MARK_FILE_NAME));

            TpchHelper.createTables(client);

            TpchHelper.fillTables(client, datasetPath);
        }
        else if (!PERSISTENT) {
            TpchHelper.createTables(client);

            TpchHelper.fillTables(client, datasetPath);
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
