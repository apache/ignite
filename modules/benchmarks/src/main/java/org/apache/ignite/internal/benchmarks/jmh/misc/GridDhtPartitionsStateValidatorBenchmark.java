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

package org.apache.ignite.internal.benchmarks.jmh.misc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionsStateValidator;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import static org.openjdk.jmh.annotations.Scope.Thread;

/** */
@State(Scope.Benchmark)
public class GridDhtPartitionsStateValidatorBenchmark extends JmhAbstractBenchmark {
    /** */
    @State(Thread)
    public static class Context {
        /** */
        private final UUID localNodeId = UUID.randomUUID();

        /** */
        private GridCacheSharedContext cctxMock;

        /** */
        private GridDhtPartitionTopology topologyMock;

        /** */
        private GridDhtPartitionsStateValidator validator;

        /** */
        private Map<UUID, GridDhtPartitionsSingleMessage> messages = new HashMap<>();

        /** */
        private UUID ignoreNode = UUID.randomUUID();

        /** */
        private static final int NODES = 3;

        /** */
        private static final int PARTS = 100;

        /**
         * @return Partition mock with specified {@code id}, {@code updateCounter} and {@code size}.
         */
        private GridDhtLocalPartition partitionMock(int id, long updateCounter, long size) {
            GridDhtLocalPartition partitionMock = Mockito.mock(GridDhtLocalPartition.class);
            Mockito.when(partitionMock.id()).thenReturn(id);
            Mockito.when(partitionMock.updateCounter()).thenReturn(updateCounter);
            Mockito.when(partitionMock.fullSize()).thenReturn(size);
            Mockito.when(partitionMock.state()).thenReturn(GridDhtPartitionState.OWNING);
            return partitionMock;
        }

        /**
         * @param countersMap Update counters map.
         * @param sizesMap Sizes map.
         * @return Message with specified {@code countersMap} and {@code sizeMap}.
         */
        private GridDhtPartitionsSingleMessage from(@Nullable Map<Integer, T2<Long, Long>> countersMap, @Nullable Map<Integer, Long> sizesMap) {
            GridDhtPartitionsSingleMessage msg = new GridDhtPartitionsSingleMessage();
            if (countersMap != null)
                msg.addPartitionUpdateCounters(0, countersMap);
            if (sizesMap != null)
                msg.addPartitionSizes(0, sizesMap);
            return msg;
        }

        /** */
        @Setup
        public void setup() {
            // Prepare mocks.
            cctxMock = Mockito.mock(GridCacheSharedContext.class);
            Mockito.when(cctxMock.localNodeId()).thenReturn(localNodeId);

            topologyMock = Mockito.mock(GridDhtPartitionTopology.class);
            Mockito.when(topologyMock.partitionState(Matchers.any(), Matchers.anyInt())).thenReturn(GridDhtPartitionState.OWNING);
            Mockito.when(topologyMock.groupId()).thenReturn(0);

            Mockito.when(topologyMock.partitions()).thenReturn(PARTS);

            List<GridDhtLocalPartition> localPartitions = Lists.newArrayList();

            Map<Integer, T2<Long, Long>> updateCountersMap = new HashMap<>();

            Map<Integer, Long> cacheSizesMap = new HashMap<>();

            IntStream.range(0, PARTS).forEach(k -> { localPartitions.add(partitionMock(k, k + 1, k + 1));
                long us = k > 20 && k <= 30 ? 0 : k + 2L;
                updateCountersMap.put(k, new T2<>(k + 2L, us));
                cacheSizesMap.put(k, us); });

            Mockito.when(topologyMock.localPartitions()).thenReturn(localPartitions);
            Mockito.when(topologyMock.currentLocalPartitions()).thenReturn(localPartitions);

            // Form single messages map.
            Map<UUID, GridDhtPartitionsSingleMessage> messages = new HashMap<>();

            for (int n = 0; n < NODES; ++n) {
                UUID remoteNode = UUID.randomUUID();

                messages.put(remoteNode, from(updateCountersMap, cacheSizesMap));
            }

            messages.put(ignoreNode, from(updateCountersMap, cacheSizesMap));

            validator = new GridDhtPartitionsStateValidator(cctxMock);
        }
    }

    /** */
    @Benchmark
    public void testValidatePartitionsUpdateCounters(Context context) {
        context.validator.validatePartitionsUpdateCounters(context.topologyMock,
                context.messages, Sets.newHashSet(context.ignoreNode));
    }

    /** */
    @Benchmark
    public void testValidatePartitionsSizes(Context context) {
        context.validator.validatePartitionsSizes(context.topologyMock, context
                .messages, Sets.newHashSet(context.ignoreNode));
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(1);
    }

    /**
     * Run benchmark.
     *
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(int threads) throws Exception {
        JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(threads)
                .warmupIterations(5)
                .measurementIterations(10)
                .benchmarks(GridDhtPartitionsStateValidatorBenchmark.class.getSimpleName())
                .jvmArguments("-XX:+UseG1GC", "-Xms4g", "-Xmx4g")
                .run();
    }
}
