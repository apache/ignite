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

package org.apache.ignite.internal.benchmarks.jmh.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.Node;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;

/**
 * Comparison of HashMap vs view on List on small sizes.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(Throughput)
public class SmallHashSetsVsReadOnlyViewBenchmark extends JmhAbstractBenchmark {
    /** */
    private static final int SIZE = AffinityAssignment.IGNITE_AFFINITY_BACKUPS_THRESHOLD;

    /** */
    private static final int PARTS = 8192;

    /**
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .threads(1)
            .measurementIterations(20)
            .benchmarks(SmallHashSetsVsReadOnlyViewBenchmark.class.getSimpleName())
            .run();
    }

    /** */
    private final Random random = new Random();

    /** */
    private final List<Collection<UUID>> hashSets = new ArrayList<>();

    /** */
    private final List<List<Node>> lists = new ArrayList<>();

    /** */
    private final Node[] nodes = new Node[SIZE];

    /** */
    @Setup
    public void setup() {
        for (int i = 0; i < SIZE; i++)
            nodes[i] = new Node(UUID.randomUUID());

        for (int i = 0; i < PARTS; i++) {
            Collection<UUID> hashSet = new HashSet<>();

            for (int j = 0; j < SIZE; j++)
                hashSet.add(nodes[j].getUuid());

            hashSets.add(hashSet);

            List<Node> list = new ArrayList<>(SIZE);

            for (int j = 0; j < SIZE; j++)
                list.add(nodes[j]);

            lists.add(list);
        }
    }

    /** */
    @Benchmark
    public boolean hashSetContainsRandom() {
        return hashSets.get(random.nextInt(PARTS))
            .contains(nodes[random.nextInt(SIZE)].getUuid());
    }

    /** */
    @Benchmark
    public boolean readOnlyViewContainsRandom() {
        return F.viewReadOnly(
            lists.get(random.nextInt(PARTS)),
            (IgniteClosure<Node, UUID>)Node::getUuid
        ).contains(nodes[random.nextInt(SIZE)].getUuid());
    }

    /** */
    @Benchmark
    public boolean hashSetIteratorRandom() {
        UUID randomUuid = nodes[random.nextInt(SIZE)].getUuid();

        Collection<UUID> col = hashSets.get(random.nextInt(PARTS));

        boolean contains = false;

        for (UUID uuid : col)
            if (randomUuid.equals(uuid))
                contains = true;

        return contains;
    }

    /** */
    @Benchmark
    public boolean readOnlyViewIteratorRandom() {
        UUID randomUuid = nodes[random.nextInt(SIZE)].getUuid();

        Collection<UUID> col = F.viewReadOnly(
            lists.get(random.nextInt(PARTS)),
            (IgniteClosure<Node, UUID>)Node::getUuid
        );

        boolean contains = false;

        for (UUID uuid : col)
            if (randomUuid.equals(uuid))
                contains = true;

        return contains;
    }
}

