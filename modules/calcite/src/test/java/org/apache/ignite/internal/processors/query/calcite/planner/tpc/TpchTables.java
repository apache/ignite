/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.planner.tpc;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Iterator;

import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.DATE;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.DECIMAL;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.INT32;
import static org.apache.ignite.internal.processors.query.calcite.planner.tpc.TpchHelper.STRING;

/**
 * Enumeration of tables from TPC-H specification.
 */
@SuppressWarnings("NonSerializableFieldInSerializableClass")
public enum TpchTables implements TpcTable {
    LINEITEM(
            new long[] {6_001_215, 5_999_989_709L, 18_000_048_306L, 59_999_994_267L, 179_999_978_268L, 599_999_969_200L},
            new Column("L_ORDERKEY", INT32),
            new Column("L_PARTKEY", INT32),
            new Column("L_SUPPKEY", INT32),
            new Column("L_LINENUMBER", INT32),
            new Column("L_QUANTITY", DECIMAL),
            new Column("L_EXTENDEDPRICE", DECIMAL),
            new Column("L_DISCOUNT", DECIMAL),
            new Column("L_TAX", DECIMAL),
            new Column("L_RETURNFLAG", STRING),
            new Column("L_LINESTATUS", STRING),
            new Column("L_SHIPDATE", DATE),
            new Column("L_COMMITDATE", DATE),
            new Column("L_RECEIPTDATE", DATE),
            new Column("L_SHIPINSTRUCT", STRING),
            new Column("L_SHIPMODE", STRING),
            new Column("L_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::integer, ?::integer,"
                    + "?::decimal(15, 2), ?::decimal(15, 2), ?::decimal(15, 2),"
                    + "?::decimal(15, 2), ?::char(1), ?::char(1), ?::date,"
                    + "?::date, ?::date, ?::char(25), ?::char(10), ?::varchar(44));";
        }
    },

    PART(
            new long[] {200_000, 200_000_000, 600_000_000, 2_000_000_000, 6_000_000_000L, 20_000_000_000L},
            new Column("P_PARTKEY", INT32),
            new Column("P_NAME", STRING),
            new Column("P_MFGR", STRING),
            new Column("P_BRAND", STRING),
            new Column("P_TYPE", STRING),
            new Column("P_SIZE", INT32),
            new Column("P_CONTAINER", STRING),
            new Column("P_RETAILPRICE", DECIMAL),
            new Column("P_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::varchar(55), ?::char(25),"
                    + "?::char(10), ?::varchar(25), ?::integer,"
                    + "?::char(10), ?::decimal(15, 2), ?::varchar(23));";
        }
    },

    SUPPLIER(
            new long[] {10_000, 10_000_000, 30_000_000, 100_000_000, 300_000_000, 1_000_000_000},
            new Column("S_SUPPKEY", INT32),
            new Column("S_NAME", STRING),
            new Column("S_ADDRESS", STRING),
            new Column("S_NATIONKEY", INT32),
            new Column("S_PHONE", STRING),
            new Column("S_ACCTBAL", DECIMAL),
            new Column("S_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::varchar(40),"
                    + "?::integer, ?::char(15), ?::decimal(15, 2), ?::varchar(101));";
        }
    },

    PARTSUPP(
            new long[] {800_000, 800_000_000, 2_400_000_000L, 8_000_000_000L, 24_000_000_000L, 80_000_000_000L},
            new Column("PS_PARTKEY", INT32),
            new Column("PS_SUPPKEY", INT32),
            new Column("PS_AVAILQTY", INT32),
            new Column("PS_SUPPLYCOST", DECIMAL),
            new Column("PS_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::integer,"
                    + "?::decimal(15, 2), ?::varchar(199));";
        }
    },

    NATION(
            new long[] {25, 25, 25, 25, 25, 25},
            new Column("N_NATIONKEY", INT32),
            new Column("N_NAME", STRING),
            new Column("N_REGIONKEY", INT32),
            new Column("N_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::integer, ?::varchar(152));";
        }
    },

    REGION(
            new long[] {5, 5, 5, 5, 5, 5},
            new Column("R_REGIONKEY", INT32),
            new Column("R_NAME", STRING),
            new Column("R_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::char(25), ?::varchar(152));";
        }
    },

    ORDERS(
            new long[] {1_500_000, 1_500_000_000, 4_500_000_000L, 15_000_000_000L, 45_000_000_000L, 150_000_000_000L},
            new Column("O_ORDERKEY", INT32),
            new Column("O_CUSTKEY", INT32),
            new Column("O_ORDERSTATUS", STRING),
            new Column("O_TOTALPRICE", DECIMAL),
            new Column("O_ORDERDATE", DATE),
            new Column("O_ORDERPRIORITY", STRING),
            new Column("O_CLERK", STRING),
            new Column("O_SHIPPRIORITY", INT32),
            new Column("O_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::integer, ?::char(1),"
                    + "?::decimal(15, 2), ?::date, ?::char(15),"
                    + "?::char(15), ?::integer, ?::varchar(79));";
        }
    },

    CUSTOMER(
            new long[] {150_000, 150_000_000, 450_000_000L, 1_500_000_000L, 4_500_000_000L, 15_000_000_000L},
            new Column("C_CUSTKEY", INT32),
            new Column("C_NAME", STRING),
            new Column("C_ADDRESS", STRING),
            new Column("C_NATIONKEY", INT32),
            new Column("C_PHONE", STRING),
            new Column("C_ACCTBAL", DECIMAL),
            new Column("C_MKTSEGMENT", STRING),
            new Column("C_COMMENT", STRING)
    ) {
        @Override public String insertPrepareStatement() {
            return "INSERT INTO " + tableName() + " VALUES ("
                    + "?::integer, ?::varchar(25), ?::varchar(40),"
                    + "?::integer, ?::char(15), ?::decimal(15, 2),"
                    + "?::char(10), ?::varchar(117));";
        }
    };

    private final Column[] columns;
    private final long[] tableSizes;

    TpchTables(long[] tableSizes, Column... columns) {
        this.tableSizes = tableSizes;
        this.columns = columns;
    }

    @Override public String tableName() {
        return name().toLowerCase();
    }

    @Override public int columnsCount() {
        return columns.length;
    }

    @Override public String columnName(int idx) {
        return columns[idx].name;
    }

    @Override public String ddlScript() {
        return TpchHelper.loadFromResource("tpch/ddl/" + tableName() + "_ddl.sql");
    }

    @SuppressWarnings("resource")
    @Override public Iterator<Object[]> dataProvider(Path pathToDataset) throws IOException {
        return Files.lines(pathToDataset.resolve(tableName() + ".tbl"))
                .map(this::csvLineToTableValues)
                .iterator();
    }

    @Override public long estimatedSize(TpcScaleFactor sf) {
        return sf.size(tableSizes);
    }

    private Object[] csvLineToTableValues(String line) {
        String[] stringValues = line.split("\\|");
        Object[] values = new Object[columns.length];

        for (int i = 0; i < columns.length; i++) {
            switch (columns[i].type) {
                case INT32:
                    values[i] = Integer.valueOf(stringValues[i]);
                    break;
                case DECIMAL:
                    values[i] = new BigDecimal(stringValues[i]);
                    break;
                case DATE:
                    values[i] = LocalDate.parse(stringValues[i]);
                    break;
                case STRING:
                    values[i] = stringValues[i];
                    break;
                default:
                    throw new IllegalStateException(columns[i].type.toString());
            }
        }

        return values;
    }

    private static class Column {
        private final String name;
        private final String type;

        private Column(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }
}
