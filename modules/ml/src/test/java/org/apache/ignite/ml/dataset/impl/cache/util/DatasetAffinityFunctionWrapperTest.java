/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link DatasetAffinityFunctionWrapper}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatasetAffinityFunctionWrapperTest {
    /** Mocked affinity function. */
    @Mock
    private AffinityFunction affinityFunction;

    /** Wrapper. */
    private DatasetAffinityFunctionWrapper wrapper;

    /** Initialization. */
    @Before
    public void beforeTest() {
        wrapper = new DatasetAffinityFunctionWrapper(affinityFunction);
    }

    /** Tests {@code reset()} method. */
    @Test
    public void testReset() {
        wrapper.reset();

        verify(affinityFunction, times(1)).reset();
    }

    /** Tests {@code partitions()} method. */
    @Test
    public void testPartitions() {
        doReturn(42).when(affinityFunction).partitions();

        int partitions = wrapper.partitions();

        assertEquals(42, partitions);
        verify(affinityFunction, times(1)).partitions();
    }

    /** Tests {@code partition} method. */
    @Test
    public void testPartition() {
        doReturn(0).when(affinityFunction).partition(eq(42));

        int part = wrapper.partition(42);

        assertEquals(42, part);
        verify(affinityFunction, times(0)).partition(any());
    }

    /** Tests {@code assignPartitions()} method. */
    @Test
    public void testAssignPartitions() {
        List<List<ClusterNode>> nodes = Collections.singletonList(Collections.singletonList(mock(ClusterNode.class)));

        doReturn(nodes).when(affinityFunction).assignPartitions(any());

        List<List<ClusterNode>> resNodes = wrapper.assignPartitions(mock(AffinityFunctionContext.class));

        assertEquals(nodes, resNodes);
        verify(affinityFunction, times(1)).assignPartitions(any());
    }

    /** Tests {@code removeNode()} method. */
    @Test
    public void testRemoveNode() {
        UUID nodeId = UUID.randomUUID();

        wrapper.removeNode(nodeId);

        verify(affinityFunction, times(1)).removeNode(eq(nodeId));
    }
}
