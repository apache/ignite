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

package org.apache.ignite.tensorflow.core.longrunning;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessClearTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessPingTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessStartTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessStopTask;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessState;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link LongRunningProcessManager}.
 */
public class LongRunningProcessManagerTest {
    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void testStart() {
        UUID nodeId = UUID.randomUUID();
        UUID procId = UUID.randomUUID();

        Ignite ignite = mock(Ignite.class);
        IgniteCluster cluster = mock(IgniteCluster.class);
        ClusterGroup clusterGrp = mock(ClusterGroup.class);
        IgniteCompute igniteCompute = mock(IgniteCompute.class);
        doReturn(cluster).when(ignite).cluster();
        doReturn(igniteCompute).when(ignite).compute(eq(clusterGrp));
        doReturn(clusterGrp).when(cluster).forNodeId(eq(nodeId));
        doReturn(Collections.singletonList(procId)).when(igniteCompute).call(any(IgniteCallable.class));

        List<LongRunningProcess> list = Collections.singletonList(new LongRunningProcess(nodeId, () -> {}));

        LongRunningProcessManager mgr = new LongRunningProcessManager(ignite);
        Map<UUID, List<UUID>> res = mgr.start(list);

        assertEquals(1, res.size());
        assertTrue(res.containsKey(nodeId));
        assertEquals(procId, res.get(nodeId).iterator().next());

        verify(igniteCompute).call(any(LongRunningProcessStartTask.class));
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void testPing() {
        UUID nodeId = UUID.randomUUID();
        UUID procId = UUID.randomUUID();

        Ignite ignite = mock(Ignite.class);
        IgniteCluster cluster = mock(IgniteCluster.class);
        ClusterGroup clusterGrp = mock(ClusterGroup.class);
        IgniteCompute igniteCompute = mock(IgniteCompute.class);
        doReturn(cluster).when(ignite).cluster();
        doReturn(igniteCompute).when(ignite).compute(eq(clusterGrp));
        doReturn(clusterGrp).when(cluster).forNodeId(eq(nodeId));
        doReturn(Collections.singletonList(new LongRunningProcessStatus(LongRunningProcessState.RUNNING)))
            .when(igniteCompute).call(any(IgniteCallable.class));

        Map<UUID, List<UUID>> procIds = new HashMap<>();
        procIds.put(nodeId, Collections.singletonList(procId));

        LongRunningProcessManager mgr = new LongRunningProcessManager(ignite);
        Map<UUID, List<LongRunningProcessStatus>> res = mgr.ping(procIds);

        assertEquals(1, res.size());
        assertTrue(res.containsKey(nodeId));
        assertEquals(LongRunningProcessState.RUNNING, res.get(nodeId).iterator().next().getState());

        verify(igniteCompute).call(any(LongRunningProcessPingTask.class));
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void testStop() {
        UUID nodeId = UUID.randomUUID();
        UUID procId = UUID.randomUUID();

        Ignite ignite = mock(Ignite.class);
        IgniteCluster cluster = mock(IgniteCluster.class);
        ClusterGroup clusterGrp = mock(ClusterGroup.class);
        IgniteCompute igniteCompute = mock(IgniteCompute.class);
        doReturn(cluster).when(ignite).cluster();
        doReturn(igniteCompute).when(ignite).compute(eq(clusterGrp));
        doReturn(clusterGrp).when(cluster).forNodeId(eq(nodeId));
        doReturn(Collections.singletonList(new LongRunningProcessStatus(LongRunningProcessState.RUNNING)))
            .when(igniteCompute).call(any(IgniteCallable.class));

        Map<UUID, List<UUID>> procIds = new HashMap<>();
        procIds.put(nodeId, Collections.singletonList(procId));

        LongRunningProcessManager mgr = new LongRunningProcessManager(ignite);
        Map<UUID, List<LongRunningProcessStatus>> res = mgr.stop(procIds, true);

        assertEquals(1, res.size());
        assertTrue(res.containsKey(nodeId));
        assertEquals(LongRunningProcessState.RUNNING, res.get(nodeId).iterator().next().getState());

        verify(igniteCompute).call(any(LongRunningProcessStopTask.class));
    }

    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void testClear() {
        UUID nodeId = UUID.randomUUID();
        UUID procId = UUID.randomUUID();

        Ignite ignite = mock(Ignite.class);
        IgniteCluster cluster = mock(IgniteCluster.class);
        ClusterGroup clusterGrp = mock(ClusterGroup.class);
        IgniteCompute igniteCompute = mock(IgniteCompute.class);
        doReturn(cluster).when(ignite).cluster();
        doReturn(igniteCompute).when(ignite).compute(eq(clusterGrp));
        doReturn(clusterGrp).when(cluster).forNodeId(eq(nodeId));
        doReturn(Collections.singletonList(new LongRunningProcessStatus(LongRunningProcessState.RUNNING)))
            .when(igniteCompute).call(any(IgniteCallable.class));

        Map<UUID, List<UUID>> procIds = new HashMap<>();
        procIds.put(nodeId, Collections.singletonList(procId));

        LongRunningProcessManager mgr = new LongRunningProcessManager(ignite);
        Map<UUID, List<LongRunningProcessStatus>> res = mgr.clear(procIds);

        assertEquals(1, res.size());
        assertTrue(res.containsKey(nodeId));
        assertEquals(LongRunningProcessState.RUNNING, res.get(nodeId).iterator().next().getState());

        verify(igniteCompute).call(any(LongRunningProcessClearTask.class));
    }
}
