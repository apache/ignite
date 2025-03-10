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

import java.io.File;
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
import org.apache.ignite.configuration.LoadAllWarmUpConfiguration;
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
// Use @Fork(value = 0) to debug or attach profiler.
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings({"unused"})
public class TpchBenchmark {
    /*
        By default, this benchmark creates a separate work directory for each scale factor value
        and engine type, like `work-CALCITE-1.0`, `work-H2-0.1`, etc.

        Also, the separate TPC-H dataset directory is created for each scale factor, like `tpch-dataset-0.01`.

        If persistence is used (it's so by default) dataset is loaded into the ignite cluster only
        once to speed up testing. Cluster is warmed-up after restart before each benchmark run to ensure stable results.

        These directories are not removed automatically and may be reused for subsequent invocations.
        Clean them yourselves if needed.
    */

    /** Count of server nodes. */
    private static final int SRV_NODES_CNT = 3;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Path to the dataset. */
    private Path datasetPath;

    /** If true the dataset will be loaded only once to speed up the testing. */
    private static final Boolean USE_PERSISTENCE = true;

    /** */
    private static final String DATASET_READY_MARK_FILE_NAME = "ready.txt";

    /** Scale factor. scale == 1.0 means about 1Gb of data. */
    @Param({"0.01", "0.1", "1.0"})
    private String scale;

    /** Query engine. */
    @Param({"CALCITE", "H2"})
    private String engine;

    /**
     * Query id.
     * <p>
     * The commented queries do not currently work with Calcite even for scale=0.01.
     * The 11, 13, 15 can not be parsed with H2.
     */
    @Param({
        "1", /*"2",*/ "3", "4", /*"5",*/
        "6", "7", /*"8",*/ /*"9",*/ "10",
        "11", "12", "13", "14", /*"15",*/
        /*"16",*/ /*"17",*/ "18", /*"19",*/ /*"20",*/
        /*"21",*/ "22"})
    private String queryId;

    /** Query SQL string. */
    private String queryString;

    /** Ignite client. */
    private Ignite client;

    /** Servers. */
    private final Ignite[] servers = new Ignite[SRV_NODES_CNT];

    /**
     * Test already planned and cached query (without the initial planning).
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 3, time = 10)
    public void cached(Blackhole bh) {
        sql(bh, queryString);
    }

    /**
     * Test a single cold non-cached query (include initial planning).
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 1)
    public void cold(Blackhole bh) {
        sql(bh, queryString);
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() throws IOException, IgniteCheckedException {
        for (int i = 0; i < SRV_NODES_CNT; i++)
            servers[i] = Ignition.start(configuration("server" + i));

        if (USE_PERSISTENCE)
            servers[0].cluster().state(ClusterState.ACTIVE);

        client = Ignition.start(configuration("client").setClientMode(true));

        queryString = TpchHelper.getQuery(Integer.parseInt(queryId));

        loadDataset();
    }

    /**
     * Stop Ignite instance.
     */
    @TearDown
    public void tearDown() {
        client.close();

        if (USE_PERSISTENCE)
            servers[0].cluster().state(ClusterState.INACTIVE);

        for (Ignite ignite : servers)
            ignite.close();
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

        if (USE_PERSISTENCE) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setWarmUpConfiguration(new LoadAllWarmUpConfiguration())));
        }

        cfg.setWorkDirectory(getWorkDirectory().toString());

        return cfg;
    }

    /**
     * Generate name of work directory
     */
    private Path getWorkDirectory() {
        return Path.of(U.getIgniteHome(), String.format("work-%s-%s", engine, scale));
    }

    /**
     * Execute several SQL queries separated by semicolons.
     */
    private void sql(Blackhole bh, String sql) {
        try {
            for (String q : sql.split(";")) {
                if (!q.trim().isEmpty()) {
                    SqlFieldsQuery qry = new SqlFieldsQuery(q.trim());

                    qry.setDistributedJoins(true);

                    try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)client).context().query().querySqlFields(qry, false)) {
                        cursor.forEach(bh::consume);
                    }
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
     * The dataset .tbl files are created only once in the created directory.
     * Subsequent runs will use previously generated dataset.
     * <p>
     * If persistent storage is used, then the dataset will be loaded to Ignite cluster only once.
     */
    private void loadDataset() throws IOException {
        datasetPath = getOrCreateDataset(scale);

        if (!USE_PERSISTENCE ||
            !Files.exists(getWorkDirectory().resolve(DATASET_READY_MARK_FILE_NAME))) {

            TpchHelper.createTables(client);

            TpchHelper.fillTables(client, datasetPath);

            if (USE_PERSISTENCE)
                Files.createFile(getWorkDirectory().resolve(DATASET_READY_MARK_FILE_NAME));
        }
    }

    /**
     * Create TPC-H dataset if it does not yet exist.
     *
     * @param scale Scale factor.
     * @return Path to the dataset directory.
     */
    private Path getOrCreateDataset(String scale) throws IOException {
        File dir = Path.of(U.getIgniteHome(), String.format("tpch-dataset-%s", scale)).toFile();

        if (!dir.exists()) {
            if (!dir.mkdirs())
                throw new RuntimeException("Failed to create dataset directory at: " + dir);
        }

        if (!Files.exists(dir.toPath().resolve(DATASET_READY_MARK_FILE_NAME))) {
            TpchHelper.generateDataset(Double.parseDouble(scale), dir.toPath());

            Files.createFile(dir.toPath().resolve(DATASET_READY_MARK_FILE_NAME));
        }

        return dir.toPath();
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
