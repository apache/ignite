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

package org.apache.ignite.internal.benchmarks;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TupleMarshallerImpl;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Serializer benchmark.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 15)
@Measurement(iterations = 1, time = 30)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(jvmArgs = "-Djava.lang.invoke.stringConcat=BC_SB" /* Workaround for Java 9+ */, value = 1)
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class TupleMarshallerFixlenOnlyBenchmark {
    /** Random. */
    private Random rnd = new Random();

    /** Tuple marshaller. */
    private TupleMarshaller marshaller;

    /** Object fields count. */
    @Param({"1", "10", "100"})
    public int fieldsCount;

    /** Nullable cols. */
    @Param({"true", "false"})
    public boolean nullable;

    /** Schema descriptor. */
    private SchemaDescriptor schema;

    /** Values. */
    private Object[] vals;

    /**
     * Runner.
     */
    public static void main(String[] args) throws RunnerException {
        new Runner(
            new OptionsBuilder()
                .include(TupleMarshallerFixlenOnlyBenchmark.class.getSimpleName())
                .build()
        ).run();
    }

    /**
     *
     */
    @Setup
    public void init() {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        schema = new SchemaDescriptor(
            UUID.randomUUID(),
            42,
            new Column[] {new Column("key", NativeTypes.INT64, false)},
            IntStream.range(0, fieldsCount).boxed()
                .map(i -> new Column("col" + i, NativeTypes.INT64, nullable))
                .toArray(Column[]::new)
        );

        marshaller = new TupleMarshallerImpl(null, null, new SchemaRegistryImpl(v -> null) {
            @Override public SchemaDescriptor schema() {
                return schema;
            }

            @Override public SchemaDescriptor schema(int ver) {
                return schema;
            }

            @Override public int lastSchemaVersion() {
                return schema.version();
            }
        });

        vals = new Object[schema.valueColumns().length()];

        for (int i = 0; i < vals.length; i++)
            vals[i] = rnd.nextLong();
    }

    /**
     * Measure tuple build then marshall.
     *
     * @param bh Black hole.
     */
    @Benchmark
    public void measureTupleBuildAndMarshallerCost(Blackhole bh) {
        final Columns cols = schema.valueColumns();

        final TupleImpl valBld = new TupleImpl();

        for (int i = 0; i < cols.length(); i++)
            valBld.set(cols.column(i).name(), vals[i]);

        Tuple keyTuple = new TupleImpl().set("key", rnd.nextLong());

        final Row row = marshaller.marshal(keyTuple, valBld);

        bh.consume(row);
    }
}
