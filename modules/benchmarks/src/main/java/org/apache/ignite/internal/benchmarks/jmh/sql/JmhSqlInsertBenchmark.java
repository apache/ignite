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

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * Benchmark for insertion operation, comparing SQL APIs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JmhSqlInsertBenchmark extends JmhSqlAbstractBenchmark {
    /** */
    private int id;

    /** */
    private static final String FIELD_VAL = "a".repeat(100);

    /** */
    private static final String TABLE_NAME = "dept";

    /** */
    private String insertStr;

    /** */
    private String multiInsertStr;

    /**
     * Initiate new tables.
     */
    @Override public void setup() {
        super.setup();

        insertStr = createInsertStatement();
        multiInsertStr = createMultiInsertStatement();

        executeSql("CREATE TABLE " + TABLE_NAME +
                "(ycsb_key int PRIMARY KEY," +
                "field1   varchar(100)," +
                "field2   varchar(100)," +
                "field3   varchar(100)," +
                "field4   varchar(100)," +
                "field5   varchar(100)," +
                "field6   varchar(100)," +
                "field7   varchar(100)," +
                "field8   varchar(100)," +
                "field9   varchar(100)," +
                "field10  varchar(100))"
        );
    }

    /**
     * Benchmark for SQL insert via embedded client.
     */
    @Benchmark
    public void sqlSimpleInsert() {
        executeSql(insertStr, id++);
    }

    /**
     * Benchmark for batch SQL insert via embedded client.
     */
    @Benchmark
    public void sqlBatchInsert() {
        executeSql(multiInsertStr, id, id + 1);

        id += 2;
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhSqlInsertBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }

    /** */
    private static String createInsertStatement() {
        /** */
        String insertQryTemplate = "insert into %s(%s, %s) values(?, %s)";

        String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
        String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

        return format(insertQryTemplate, TABLE_NAME, "ycsb_key", fieldsQ, valQ);
    }

    /** */
    private static String createMultiInsertStatement() {
        /** */
        String insertQryTemplate = "insert into %s(%s, %s) values(?, %s), (?, %s)";

        String fieldsQ = IntStream.range(1, 11).mapToObj(i -> "field" + i).collect(joining(","));
        String valQ = IntStream.range(1, 11).mapToObj(i -> "'" + FIELD_VAL + "'").collect(joining(","));

        return format(insertQryTemplate, TABLE_NAME, "ycsb_key", fieldsQ, valQ, valQ);
    }
}
