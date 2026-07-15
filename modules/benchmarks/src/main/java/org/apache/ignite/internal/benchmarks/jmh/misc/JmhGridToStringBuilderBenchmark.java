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
 * and limitations under the License.
 */

package org.apache.ignite.internal.benchmarks.jmh.misc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks {@link GridToStringBuilder} on deep/wide graphs.
 *
 * <p>Measures the cost of {@code GridToStringBuilder.toString()} under various graph shapes:
 * <ul>
 *   <li>Deep nesting (100 levels of parent→child)</li>
 *   <li>Wide objects (50 fields per object)</li>
 *   <li>Deep + wide (100 levels × 50 fields)</li>
 *   <li>Wide collections (1000-element list inside single object)</li>
 *   <li>Wide maps (1000-entry map inside single object)</li>
 *   <li>Self-referencing (recursion detection)</li>
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class JmhGridToStringBuilderBenchmark {

    /* ── graph data ── */

    /** Deep chain: 100 levels of nesting. */
    private Object deepGraph;

    /** Wide object: 1 object with 50 scalar fields (carried in a Map). */
    private Object wideGraph;

    /** Deep + wide: 100 levels, each with 50 scalar fields. */
    private Object deepWideGraph;

    /** Wide collection: single object containing a list of 1000 simple objects. */
    private Object wideCollGraph;

    /** Wide map: single object containing a map of 1000 simple entries. */
    private Object wideMapGraph;

    /** Self-referencing object (recursion detection path). */
    private Object recursiveGraph;

    /**
     * Simple POJO with a single reference — used for deep/deep-wide graphs.
     */
    private static class Node {
        final String val;
        final Map<String, Object> fields;
        Node next;

        Node(String val, Map<String, Object> fields, Node next) {
            this.val = val;
            this.fields = fields;
            this.next = next;
        }
    }

    /** Simple POJO for collections. */
    private static class Item {
        final String name;
        final int value;

        Item(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    /** Container with a collection. */
    private static class CollHolder {
        final List<Item> items;
        CollHolder(List<Item> items) { this.items = items; }
    }

    /** Container with a map. */
    private static class MapHolder {
        final Map<String, Item> items;
        MapHolder(Map<String, Item> items) { this.items = items; }
    }

    /* ── setup ── */

    @Setup(Level.Iteration)
    public void setup() {
        // 1. Deep graph (100 levels, 1 field each)
        deepGraph = buildDeepGraph(100, 1);

        // 2. Wide graph (1 level, 50 fields)
        wideGraph = buildNodeWithFields(50);

        // 3. Deep + wide (100 levels × 50 fields)
        deepWideGraph = buildDeepGraph(100, 50);

        // 4. Wide collection (1000 items)
        List<Item> coll = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++)
            coll.add(new Item("item" + i, i));
        wideCollGraph = new CollHolder(coll);

        // 5. Wide map (1000 entries)
        Map<String, Item> map = new LinkedHashMap<>(1000);
        for (int i = 0; i < 1000; i++)
            map.put("key" + i, new Item("val" + i, i));
        wideMapGraph = new MapHolder(map);

        // 6. Recursive (self-reference)
        recursiveGraph = new Node("self", new HashMap<>(), null);
        ((Node) recursiveGraph).next = (Node) recursiveGraph;
    }

    private static Node buildDeepGraph(int depth, int fieldsPerLevel) {
        Node tail = new Node("n" + (depth - 1), fieldsPerLevel == 1 ? null : scalarFields(fieldsPerLevel), null);
        for (int i = depth - 2; i >= 0; i--)
            tail = new Node("n" + i, i < depth - 1 && fieldsPerLevel > 1 ? scalarFields(fieldsPerLevel) : null, tail);
        return tail;
    }

    private static Node buildNodeWithFields(int fields) {
        return new Node("wide", scalarFields(fields), null);
    }

    private static Map<String, Object> scalarFields(int n) {
        Map<String, Object> m = new LinkedHashMap<>(n);
        for (int i = 0; i < n; i++)
            m.put("f" + i, "value" + i);
        return m;
    }

    /* ── benchmarks ── */

    /** Deep graph: 100 levels of nesting. */
    @Benchmark
    public void deep(Blackhole bh) {
        bh.consume(GridToStringBuilder.toString(deepGraph));
    }

    /** Wide object: 50 scalar fields. */
    @Benchmark
    public void wide(Blackhole bh) {
        bh.consume(GridToStringBuilder.toString(wideGraph));
    }

    /** Deep + wide: 100 levels × 50 fields. */
    @Benchmark
    public void deepWide(Blackhole bh) {
        bh.consume(GridToStringBuilder.toString(deepWideGraph));
    }

    /** Wide collection: 1000-element list. */
    @Benchmark
    public void wideCollection(Blackhole bh) {
        bh.consume(GridToStringBuilder.toString(wideCollGraph));
    }

    /** Wide map: 1000-entry map. */
    @Benchmark
    public void wideMap(Blackhole bh) {
        bh.consume(GridToStringBuilder.toString(wideMapGraph));
    }

    /** Self-referencing object (recursion detection). */
    @Benchmark
    public void recursive(Blackhole bh) {
        bh.consume(GridToStringBuilder.toString(recursiveGraph));
    }

    /* ── main ── */

    /**
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .benchmarks(JmhGridToStringBuilderBenchmark.class.getSimpleName())
            .run();
    }
}
