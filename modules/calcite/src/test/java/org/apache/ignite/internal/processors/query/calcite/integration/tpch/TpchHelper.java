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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import io.trino.tpch.CustomerGenerator;
import io.trino.tpch.LineItemGenerator;
import io.trino.tpch.NationGenerator;
import io.trino.tpch.OrderGenerator;
import io.trino.tpch.PartGenerator;
import io.trino.tpch.PartSupplierGenerator;
import io.trino.tpch.RegionGenerator;
import io.trino.tpch.SupplierGenerator;
import io.trino.tpch.TpchEntity;
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
            if (inputStream != null)
                exec(ignite, new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
            else
                throw new RuntimeException("Failed to read create_tables.sql: not found in resources");
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read create_tables.sql from resources", e);
        }
    }

    public static void fillTables(Ignite ignite, double scale) throws IOException {
        fillTable(ignite, "nation", new NationGenerator(), TpchHelper::nation);
        fillTable(ignite, "region", new RegionGenerator(), TpchHelper::region);
        fillTable(ignite, "part", new PartGenerator(scale,1 ,1), TpchHelper::part);
        fillTable(ignite, "supplier", new SupplierGenerator(scale,1 ,1), TpchHelper::supplier);
        fillTable(ignite, "partsupp", new PartSupplierGenerator(scale,1 ,1), TpchHelper::partsupp);
        fillTable(ignite, "customer", new CustomerGenerator(scale,1 ,1), TpchHelper::customer);
        fillTable(ignite, "orders", new OrderGenerator(scale,1 ,1), TpchHelper::orders);
        fillTable(ignite, "lineitem", new LineItemGenerator(scale,1 ,1), TpchHelper::lineitem);
    }

    /** */
    public static void generateData(double scale, Path datasetDir) throws IOException {
        fillFile(datasetDir.resolve("nation.tbl"), new NationGenerator());
        fillFile(datasetDir.resolve("region.tbl"), new RegionGenerator());
        fillFile(datasetDir.resolve("part.tbl"), new PartGenerator(scale,1 ,1));
        fillFile(datasetDir.resolve("supplier.tbl"), new SupplierGenerator(scale,1 ,1));
        fillFile(datasetDir.resolve("partsupp.tbl"), new PartSupplierGenerator(scale,1 ,1));
        fillFile(datasetDir.resolve("customer.tbl"), new CustomerGenerator(scale,1 ,1));
        fillFile(datasetDir.resolve("orders.tbl"), new OrderGenerator(scale,1 ,1));
        fillFile(datasetDir.resolve("lineitem.tbl"), new LineItemGenerator(scale,1 ,1));
    }

    private static class FileIterable implements Iterable<TpchLine> {
        private final BufferedReader reader;

        public FileIterable(Path file) throws IOException {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile()), StandardCharsets.UTF_8));
        }

        @Override
        public Iterator<TpchLine> iterator() {
            return reader.lines().map(TpchLine::new).iterator();

//            return new Iterator<>() {
//                @Override
//                public boolean hasNext() {
//                    return true;
//                }
//
//                @Override
//                public TpchEntity next() {
//                    try {
//                        String line = reader.readLine();
//
//                        if (line == null)
//                            throw new NoSuchElementException();
//
//                        return new TpchLine(line);
//                    }
//                    catch (IOException e) {
//                        throw new NoSuchElementException(e.getMessage());
//                    }
//                }
//            };
        }
    }

    private static class TpchLine implements TpchEntity {
        /** */
        private final String line;

        /** */
        public TpchLine(String line) {
            this.line = line;
        }

        @Override
        public long getRowNumber() {
            return 0;
        }

        @Override
        public String toLine() {
            return line;
        }
    }

    /**
     * Fill TPC-H tables reading dataset from the directory provided.
     *
     * @param ignite Ignite instance.
     * @param datasetDir Directory containing .tbl files with data.
     */
    public static void fillTables(Ignite ignite, Path datasetDir) {
        try {
            fillTable(ignite, "nation", new FileIterable(datasetDir.resolve("nation.tbl")), TpchHelper::nation);
            fillTable(ignite, "region", new FileIterable(datasetDir.resolve("region.tbl")), TpchHelper::region);
            fillTable(ignite, "part", new FileIterable(datasetDir.resolve("part.tbl")),   TpchHelper::part);
            fillTable(ignite, "supplier", new FileIterable(datasetDir.resolve("supplier.tbl")), TpchHelper::supplier);
            fillTable(ignite, "partsupp", new FileIterable(datasetDir.resolve("partsupp.tbl")), TpchHelper::partsupp);
            fillTable(ignite, "customer", new FileIterable(datasetDir.resolve("customer.tbl")), TpchHelper::customer);
            fillTable(ignite, "orders", new FileIterable(datasetDir.resolve("orders.tbl")),   TpchHelper::orders);
            fillTable(ignite, "lineitem", new FileIterable(datasetDir.resolve("lineitem.tbl")), TpchHelper::lineitem);
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
     * Read data from the generator provided and fill single TPC-H table.
     *
     * @param ignite Ignite instance.
     * @param table Table name.
     * @param dataGen Iterable generating one data line.
     * @param entryGen Function converting one data line to key/value entry.
     */
    private static void fillTable(Ignite ignite, String table, Iterable<? extends TpchEntity> dataGen,
                                  BiFunction<Ignite, String, GridMapEntry<?, ?>> entryGen) throws IOException {
        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(table)) {
            for (TpchEntity line: dataGen) {
                GridMapEntry<?, ?> entry = entryGen.apply(ignite, line.toLine());

                ds.addData(entry.getKey(), entry.getValue());
            }
        }
    }

    /** */
    private static void fillFile(Path file, Iterable<? extends TpchEntity> dataGen) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(file);

        for (TpchEntity entity : dataGen) {
            writer.write(entity.toLine());

            writer.newLine();
        }

        writer.close();
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
     * Execute SQL queries.
     *
     * @param ignite Ignite instance.
     * @param sql list of SQL queries, separated by semicolons.
     */
    private static void exec(Ignite ignite, String sql) {
        for (String q : sql.split(";")) {
            if (!q.trim().isEmpty()) {
                SqlFieldsQuery qry = new SqlFieldsQuery(q);

                try (QueryCursor<List<?>> cursor = ((IgniteEx)ignite).context().query().querySqlFields(qry, false)) {
                    cursor.getAll();
                }
                catch (IgniteException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
