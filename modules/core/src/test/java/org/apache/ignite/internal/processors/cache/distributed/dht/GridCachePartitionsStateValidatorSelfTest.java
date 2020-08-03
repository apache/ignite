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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionsStateValidator;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Test correct behaviour of {@link GridDhtPartitionsStateValidator} class.
 */
public class GridCachePartitionsStateValidatorSelfTest extends GridCommonAbstractTest {
    /** Mocks and stubs. */
    private final UUID localNodeId = UUID.randomUUID();

    /** */
    private GridCacheSharedContext cctxMock;

    /** */
    private GridDhtPartitionTopology topologyMock;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // Prepare mocks.
        cctxMock = Mockito.mock(GridCacheSharedContext.class);
        Mockito.when(cctxMock.localNodeId()).thenReturn(localNodeId);

        topologyMock = Mockito.mock(GridDhtPartitionTopology.class);
        Mockito.when(topologyMock.partitionState(Matchers.any(), Matchers.anyInt())).thenReturn(GridDhtPartitionState.OWNING);
        Mockito.when(topologyMock.groupId()).thenReturn(0);
        Mockito.when(topologyMock.partitions()).thenReturn(3);

        List<GridDhtLocalPartition> localPartitions = Lists.newArrayList(
            partitionMock(0, 1, 1),
            partitionMock(1, 2, 2),
            partitionMock(2, 3, 3)
        );

        Mockito.when(topologyMock.localPartitions()).thenReturn(localPartitions);
        Mockito.when(topologyMock.currentLocalPartitions()).thenReturn(localPartitions);
    }

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

    /**
     * Test partition update counters validation.
     */
    @Test
    public void testPartitionCountersValidation() {
        UUID remoteNode = UUID.randomUUID();
        UUID ignoreNode = UUID.randomUUID();

        // For partitions 0 and 2 we have inconsistent update counters.
        Map<Integer, T2<Long, Long>> updateCountersMap = new HashMap<>();
        updateCountersMap.put(0, new T2<>(2L, 2L));
        updateCountersMap.put(1, new T2<>(2L, 2L));
        updateCountersMap.put(2, new T2<>(5L, 5L));

        // For partitions 0 and 2 we have inconsistent cache sizes.
        Map<Integer, Long> cacheSizesMap = new HashMap<>();
        cacheSizesMap.put(0, 2L);
        cacheSizesMap.put(1, 2L);
        cacheSizesMap.put(2, 2L);

        // Form single messages map.
        Map<UUID, GridDhtPartitionsSingleMessage> messages = new HashMap<>();
        messages.put(remoteNode, from(updateCountersMap, cacheSizesMap));
        messages.put(ignoreNode, from(updateCountersMap, cacheSizesMap));

        GridDhtPartitionsStateValidator validator = new GridDhtPartitionsStateValidator(cctxMock);

        // (partId, (nodeId, updateCounter))
        Map<Integer, Map<UUID, Long>> result = validator.validatePartitionsUpdateCounters(topologyMock, messages, Sets.newHashSet(ignoreNode));

        // Check that validation result contains all necessary information.
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey(0));
        Assert.assertTrue(result.containsKey(2));
        Assert.assertTrue(result.get(0).get(localNodeId) == 1L);
        Assert.assertTrue(result.get(0).get(remoteNode) == 2L);
        Assert.assertTrue(result.get(2).get(localNodeId) == 3L);
        Assert.assertTrue(result.get(2).get(remoteNode) == 5L);
    }

    /**
     * Test partition cache sizes validation.
     */
    @Test
    public void testPartitionCacheSizesValidation() {
        UUID remoteNode = UUID.randomUUID();
        UUID ignoreNode = UUID.randomUUID();

        // For partitions 0 and 2 we have inconsistent update counters.
        Map<Integer, T2<Long, Long>> updateCountersMap = new HashMap<>();
        updateCountersMap.put(0, new T2<>(2L, 2L));
        updateCountersMap.put(1, new T2<>(2L, 2L));
        updateCountersMap.put(2, new T2<>(5L, 5L));

        // For partitions 0 and 2 we have inconsistent cache sizes.
        Map<Integer, Long> cacheSizesMap = new HashMap<>();
        cacheSizesMap.put(0, 2L);
        cacheSizesMap.put(1, 2L);
        cacheSizesMap.put(2, 2L);

        // Form single messages map.
        Map<UUID, GridDhtPartitionsSingleMessage> messages = new HashMap<>();
        messages.put(remoteNode, from(updateCountersMap, cacheSizesMap));
        messages.put(ignoreNode, from(updateCountersMap, cacheSizesMap));

        GridDhtPartitionsStateValidator validator = new GridDhtPartitionsStateValidator(cctxMock);

        // (partId, (nodeId, cacheSize))
        Map<Integer, Map<UUID, Long>> result = validator.validatePartitionsSizes(topologyMock, messages, Sets.newHashSet(ignoreNode));

        // Check that validation result contains all necessary information.
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey(0));
        Assert.assertTrue(result.containsKey(2));
        Assert.assertTrue(result.get(0).get(localNodeId) == 1L);
        Assert.assertTrue(result.get(0).get(remoteNode) == 2L);
        Assert.assertTrue(result.get(2).get(localNodeId) == 3L);
        Assert.assertTrue(result.get(2).get(remoteNode) == 2L);
    }
}
