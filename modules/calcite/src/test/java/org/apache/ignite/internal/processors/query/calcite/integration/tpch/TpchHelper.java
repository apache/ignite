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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridMapEntry;

/**
 * Provides utility methods to work with data and queries defined by the TPC-H benchmark.
 */
public final class TpchHelper {
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
            if (inputStream != null)
                exec(ignite, new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
            else
                throw new RuntimeException("Failed to read create_tables.sql: not found in resources");
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read create_tables.sql from resources", e);
        }
    }

    /**
     * Fill TPC-H tables with test data.
     *
     * @param ignite Ignite instance.
     * @param datasetDir Directory containing .tbl files with data.
     */
    public static void fillTables(Ignite ignite, String datasetDir) {
        try {
            fillTable(ignite, "nation", Path.of(datasetDir, "nation.tbl"), TpchHelper::nation);
            fillTable(ignite, "region", Path.of(datasetDir, "region.tbl"), TpchHelper::region);
            fillTable(ignite, "part", Path.of(datasetDir, "part.tbl"), TpchHelper::part);
            fillTable(ignite, "supplier", Path.of(datasetDir, "supplier.tbl"), TpchHelper::supplier);
            fillTable(ignite, "partsupp", Path.of(datasetDir, "partsupp.tbl"), TpchHelper::partsupp);
            fillTable(ignite, "customer", Path.of(datasetDir, "customer.tbl"), TpchHelper::customer);
            fillTable(ignite, "orders", Path.of(datasetDir, "orders.tbl"), TpchHelper::orders);
            fillTable(ignite, "lineitem", Path.of(datasetDir, "lineitem.tbl"), TpchHelper::lineitem);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return a TPC-H query given a query identifier.
     *
     * @param queryId Query identifier.
     */
    public static String getQuery(int queryId) {
        try (InputStream inputStream = TpchHelper.class.getResourceAsStream(String.format("q%d.sql", queryId))) {
            if (inputStream != null)
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            else
                throw new RuntimeException(String.format("Query Q%d is not found in resources", queryId));
        }
        catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read query Q%d from resources", queryId), e);
        }
    }

    /**
     * Read data from the .tbl file and fill single TPC-H table.
     *
     * @param ignite Ignite instance.
     * @param table Table name.
     * @param file Path to the .tbl data file.
     * @param genEntry Function converting one line from the .tbl file to key/value entry.
     */
    private static void fillTable(Ignite ignite, String table, Path file, BiFunction<Ignite, String, GridMapEntry<?, ?>> genEntry) throws IOException {
        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(table)) {
            FileInputStream inputStream = new FileInputStream(file.toFile());

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;

            while ((line = reader.readLine()) != null) {
                GridMapEntry<?, ?> entry = genEntry.apply(ignite, line);

                ds.addData(entry.getKey(), entry.getValue());
            }

            reader.close();
        }
    }

    /** */
    private static GridMapEntry<?, ?> nation(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("nation");

        builder.setField("n_nationkey", Integer.parseInt(split[0]));
        builder.setField("n_name", split[1]);
        builder.setField("n_regionkey", Integer.parseInt(split[2]));
        builder.setField("n_comment", split[3]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** */
    private static GridMapEntry<?, ?> region(Ignite ignite, String line) {
        String[] split = line.split("\\|");

        BinaryObjectBuilder builder = ignite.binary().builder("region");

        builder.setField("r_regionkey", Integer.parseInt(split[0]));
        builder.setField("r_name", split[1]);
        builder.setField("r_comment", split[2]);

        return new GridMapEntry<>(Integer.parseInt(split[0]), builder.build());
    }

    /** */
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

    /** */
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

    /** */
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

    /** */
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

    /** */
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

    /** */
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
        builder.setField("l_receiptdate",java.sql.Date.valueOf( split[12]));
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
     * Execute SQL queries.
     *
     * @param ignite Ignite instance.
     * @param sql list of SQL queries, separated by semicolons.
     */
    private static void exec(Ignite ignite, String sql) {
        for (String q : sql.split(";")) {
            if (!q.trim().isEmpty()) {
                SqlFieldsQuery query = new SqlFieldsQuery(q);

                try (QueryCursor<List<?>> cursor = ((IgniteEx)ignite).context().query().querySqlFields(query, false)) {
                    cursor.getAll();
                }
                catch (IgniteException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
