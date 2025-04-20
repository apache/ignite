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

package org.apache.ignite.internal.processors.query.calcite.integration.tpch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static io.trino.tpch.TpchTable.getTables;

/**
 * Provides utility methods to work with data and queries defined by the TPC-H benchmark.
 */
public class TpchHelper {
    /** */
    private TpchHelper() {
        // No-op.
    }

    /**
     * Create TPC-H tables.
     *
     * @param ignite Ignite instance.
     */
    public static void createTables(Ignite ignite) {
        try (InputStream inputStream = TpchHelper.class.getResourceAsStream("ddl.sql")) {
            if (inputStream == null)
                throw new RuntimeException("Failed to create TPC-H tables: ddl.sql not found in resources");

            for (String q : new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                if (!q.trim().isEmpty())
                    sql(ignite, q.trim());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create TPC-H tables: can not read ddl.sql from resources", e);
        }
    }

    /**
     * Fill TPC-H tables generating data on the fly with the scale factor provided.
     *
     * @param ignite Ignite instance.
     * @param scale Scale factor.
     */
    public static void fillTables(Ignite ignite, double scale) {
        for (TpchTable<?> table : getTables()) {
            fillTable(ignite, table.getTableName(), streamFromGenerator(table.createGenerator(scale, 1, 1)),
                BUILDER_BY_NAME.get(table.getTableName()));
        }
    }

    /**
     * Fill TPC-H tables reading dataset from the directory provided.
     *
     * @param ignite Ignite instance.
     * @param datasetDir Directory containing .tbl files with data.
     */
    public static void fillTables(Ignite ignite, Path datasetDir) throws IOException {
        for (TpchTable<?> table : getTables()) {
            fillTable(ignite, table.getTableName(), streamFromFile(datasetDir.resolve(String.format("%s.tbl", table.getTableName()))),
                BUILDER_BY_NAME.get(table.getTableName()));
        }
    }

    /**
     * Collect statistics for TPC-H tables with default 10 mins timeout.
     *
     * @param ignite Ignite instance.
     */
    public static void collectSqlStatistics(Ignite ignite) throws IgniteInterruptedCheckedException {
        collectSqlStatistics(ignite, 10 * 60 * 1000);
    }

    /**
     * Collect statistics for TPC-H tables.
     *
     * @param ignite Ignite instance.
     * @param timeoutMs Timeout in milliseconds.
     */
    public static void collectSqlStatistics(Ignite ignite, int timeoutMs) throws IgniteInterruptedCheckedException {
        for (TpchTable<?> table : getTables()) {
            if (allServersHaveStatistics(ignite, table.getTableName()))
                ignite.log().info("Table " + table.getTableName() + " already has statistics");
            else
                collectSqlStatistics(ignite, table.getTableName(), timeoutMs);
        }
    }

    /**
     * Generate TPC-H dataset for the scale factor provided. Save .tbl files in the directory provided.
     *
     * @param scale Scale factor.
     * @param datasetDir Directory to save dataset to.
     */
    public static void generateDataset(double scale, Path datasetDir) throws IOException {
        for (TpchTable<?> table : getTables()) {
            fillFile(datasetDir.resolve(String.format("%s.tbl", table.getTableName())),
                streamFromGenerator(table.createGenerator(scale, 1, 1)));
        }
    }

    /**
     * Return a TPC-H query given a query identifier.
     *
     * @param queryId Query identifier.
     */
    public static String getQuery(int queryId) {
        try (InputStream inputStream = TpchHelper.class.getResourceAsStream(String.format("q%d.sql", queryId))) {
            if (inputStream == null)
                throw new RuntimeException(String.format("Query Q%d is not found in resources", queryId));

            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read query Q%d from resources", queryId), e);
        }
    }

    /**
     * Read data lines from the stream provided and fill a single TPC-H table.
     *
     * @param ignite Ignite instance.
     * @param table Table name.
     * @param data Stream of data lines.
     * @param entryGen Function converting one data line to key/value entry.
     */
    private static void fillTable(Ignite ignite, String table, Stream<String> data,
                                  BiFunction<Ignite, String, GridMapEntry<?, ?>> entryGen) {
        ignite.log().info("Filling table: " + table + " ...");

        LongAdder cnt = new LongAdder();

        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(table)) {
            data.forEach(line -> {
                try {
                    GridMapEntry<?, ?> entry = entryGen.apply(ignite, line);

                    ds.addData(entry.getKey(), entry.getValue());

                    cnt.increment();
                }
                catch (Exception e) {
                    ignite.log().error(e.getMessage(), e);
                }
            });
        }

        ignite.log().info("Table: " + table + " is filled, cnt: " + cnt.sum());
    }

    /**
     * Read data lines from the stream provided and save them to the file.
     *
     * @param file File to save data to.
     * @param data Stream of data lines.
     */
    private static void fillFile(Path file, Stream<String> data) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            data.forEach(line -> {
                try {
                    writer.write(line);

                    writer.newLine();
                }
                catch (IOException e) {
                    throw new RuntimeException(String.format("Failed to save data to %s", file), e);
                }
            });
        }
    }

    /** List of entity builders for each table. */
    private static final Map<String, BiFunction<Ignite, String, GridMapEntry<?, ?>>> BUILDER_BY_NAME = Map.of(
        "nation", TpchHelper::nation,
        "region", TpchHelper::region,
        "part", TpchHelper::part,
        "supplier", TpchHelper::supplier,
        "partsupp", TpchHelper::partsupp,
        "customer", TpchHelper::customer,
        "orders", TpchHelper::orders,
        "lineitem", TpchHelper::lineitem
    );

    /** Nation entity builder. */
    private static GridMapEntry<?, ?> nation(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("nation");

        builder.setField("n_nationkey", Integer.parseInt(split[0]));
        builder.setField("n_name", split[1]);
        builder.setField("n_regionkey", Integer.parseInt(split[2]));
        builder.setField("n_comment", split[3]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** Region entity builder. */
    private static GridMapEntry<?, ?> region(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("region");

        builder.setField("r_regionkey", Integer.parseInt(split[0]));
        builder.setField("r_name", split[1]);
        builder.setField("r_comment", split[2]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** Part entity builder. */
    private static GridMapEntry<?, ?> part(Ignite ignite, String line) {
        String[] split = line.split("\\|");
        BinaryObjectBuilder builder = ignite.binary().builder("part");

        builder.setField("p_partkey", Integer.parseInt(split[0]));
        builder.setField("p_name", split[1]);
        builder.setField("p_mfgr", split[2]);
        builder.setField("p_brand", split[3]);
        builder.setField("p_type", split[4]);
        builder.setField("p_size", Integer.parseInt(split[5]));
        builder.setField("p_container", split[6]);
        builder.setField("p_retailprice", BigDecimal.valueOf(Double.parseDouble(split[7])));
        builder.setField("p_comment", split[8]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** Supplier entity builder. */
    private static GridMapEntry<?, ?> supplier(Ignite ignite, String line) {
        String[] split = line.split("\\|");
        BinaryObjectBuilder builder = ignite.binary().builder("supplier");

        builder.setField("s_suppkey", Integer.parseInt(split[0]));
        builder.setField("s_name", split[1]);
        builder.setField("s_address", split[2]);
        builder.setField("s_nationkey", Integer.parseInt(split[3]));
        builder.setField("s_phone", split[4]);
        builder.setField("s_acctbal", BigDecimal.valueOf(Double.parseDouble(split[5])));
        builder.setField("s_comment", split[6]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** PartSupp entity builder. */
    private static GridMapEntry<?, ?> partsupp(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("partsupp");

        builder.setField("ps_partkey", Integer.parseInt(split[0]));
        builder.setField("ps_suppkey", Integer.parseInt(split[1]));
        builder.setField("ps_availqty", Integer.parseInt(split[2]));
        builder.setField("ps_supplycost", BigDecimal.valueOf(Double.parseDouble(split[3])));
        builder.setField("ps_comment", split[4]);

        return new GridMapEntry<>(ignite.binary().builder("partsupp_key")
            .setField("ps_partkey", Integer.parseInt(split[0]))
            .setField("ps_suppkey", Integer.parseInt(split[1]))
            .build(),
            builder.build());
    }

    /** Customer entity builder. */
    private static GridMapEntry<?, ?> customer(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("customer");

        builder.setField("c_custkey", Integer.parseInt(split[0]));
        builder.setField("c_name", split[1]);
        builder.setField("c_address", split[2]);
        builder.setField("c_nationkey", Integer.parseInt(split[3]));
        builder.setField("c_phone", split[4]);
        builder.setField("c_acctbal", BigDecimal.valueOf(Double.parseDouble(split[5])));
        builder.setField("c_mktsegment", split[6]);
        builder.setField("c_comment", split[7]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** Orders entity builder. */
    private static GridMapEntry<?, ?> orders(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("orders");

        builder.setField("o_orderkey", Integer.parseInt(split[0]));
        builder.setField("o_custkey", Integer.parseInt(split[1]));
        builder.setField("o_orderstatus", split[2]);
        builder.setField("o_totalprice", BigDecimal.valueOf(Double.parseDouble(split[3])));
        builder.setField("o_orderdate", java.sql.Date.valueOf(split[4]));
        builder.setField("o_orderpriority", split[5]);
        builder.setField("o_clerk", split[6]);
        builder.setField("o_shippriority", Integer.parseInt(split[7]));
        builder.setField("o_comment", split[8]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** LineItem entity builder. */
    private static GridMapEntry<?, ?> lineitem(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("lineitem");

        builder.setField("l_orderkey", Integer.parseInt(split[0]));
        builder.setField("l_partkey", Integer.parseInt(split[1]));
        builder.setField("l_suppkey", Integer.parseInt(split[2]));
        builder.setField("l_linenumber", Integer.parseInt(split[3]));
        builder.setField("l_quantity", BigDecimal.valueOf(Double.parseDouble(split[4])));
        builder.setField("l_extendedprice", BigDecimal.valueOf(Double.parseDouble(split[5])));
        builder.setField("l_discount", BigDecimal.valueOf(Double.parseDouble(split[6])));
        builder.setField("l_tax", BigDecimal.valueOf(Double.parseDouble(split[7])));
        builder.setField("l_returnflag", split[8]);
        builder.setField("l_linestatus", split[9]);
        builder.setField("l_shipdate", java.sql.Date.valueOf(split[10]));
        builder.setField("l_commitdate", java.sql.Date.valueOf(split[11]));
        builder.setField("l_receiptdate", java.sql.Date.valueOf( split[12]));
        builder.setField("l_shipinstruct", split[13]);
        builder.setField("l_shipmode", split[14]);
        builder.setField("l_comment", split[15]);

        return new GridMapEntry<>(
            ignite.binary().builder("lineitem_key")
                .setField("l_orderkey", Integer.parseInt(split[0]))
                .setField("l_linenumber", Integer.parseInt(split[3]))
                .build(),
            builder.build()
        );
    }

    /**
     * Convert TPC-H data generator to stream of data lines.
     */
    private static Stream<String> streamFromGenerator(Iterable<? extends TpchEntity> gen) {
        return StreamSupport.stream(gen.spliterator(), false).map(TpchEntity::toLine);
    }

    /**
     * Create stream to read file line by line.
     */
    private static Stream<String> streamFromFile(Path file) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile()), StandardCharsets.UTF_8)).lines();
    }

    /**
     * Build statistics for table.
     *
     * @param ignite Ignite.
     * @param tableName SQL table name.
     */
    private static void collectSqlStatistics(Ignite ignite, String tableName, int timeoutMs) throws IgniteInterruptedCheckedException {
        ignite.log().info("Collecting statistics for table: " + tableName + " ...");

        sql(ignite, "ANALYZE " + tableName);

        long endTime = U.currentTimeMillis() + timeoutMs;
        long endTime0 = endTime < 0 ? Long.MAX_VALUE : endTime;

        waitForCondition(
            () -> allServersHaveStatistics(ignite, tableName),
            () -> U.currentTimeMillis() < endTime0,
            200);

        ignite.log().info("Statistics collected for table: " + tableName);
    }

    /**
     * @param cond Condition to wait for.
     * @param wait Wait predicate.
     * @param checkInterval Time interval between two consecutive condition checks.
     * @return {@code true} if condition was achieved, {@code false} otherwise.
     */
    private static boolean waitForCondition(
        BooleanSupplier cond,
        BooleanSupplier wait,
        long checkInterval
    ) throws IgniteInterruptedCheckedException {
        while (wait.getAsBoolean()) {
            if (cond.getAsBoolean())
                return true;

            U.sleep(checkInterval);
        }

        return false;
    }

    /**
     * Check if all server nodes have SQL statistics for the table.
     *
     * @param ignite Ignite.
     * @param tableName Table name.
     */
    private static boolean allServersHaveStatistics(Ignite ignite, String tableName) {
        return ignite.compute(ignite.cluster().forServers()).broadcast(new IgniteCallable<Boolean>() {
            @IgniteInstanceResource
            Ignite node;

            @Override public Boolean call() {
                return !F.isEmpty(sql(node, "select * from sys.statistics_local_data where name = ?", tableName.toUpperCase()));
            }
        }).stream().allMatch(a -> a);
    }

    /**
     * Execute SQL query.
     *
     * @param ignite Ignite.
     * @param sql SQL query.
     * @param params Query parameters.
     */
    private static List<List<?>> sql(Ignite ignite, String sql, Object... params) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(params);

        try (FieldsQueryCursor<List<?>> cur = ((IgniteEx)ignite).context().query().querySqlFields(qry, false)) {
            return cur.getAll();
        }
    }
}
