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

package org.apache.ignite.internal.cluster;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cluster activation events.
 */
public class ClusterActivationEventTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterActivation() throws Exception {
        checkClusterActivation(true, EventType.EVT_CLUSTER_ACTIVATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDeactivation() throws Exception {
        checkClusterActivation(false, EventType.EVT_CLUSTER_DEACTIVATED);
    }

    /**
     * @param active If {@code True} start activation process. If {@code False} start deactivation process.
     * @param evtType Expected event type.
     * @throws Exception If failed.
     */
    private void checkClusterActivation(boolean active, int evtType) throws Exception {
        try {
            Ignite ignite = startGrid(0);

            IgniteCluster cluster = ignite.cluster();

            cluster.active(active);

            Collection<Event> evts = ignite.events(cluster).remoteQuery(F.alwaysTrue(), 0, evtType);

            assert evts != null;
            assert evts.size() == 1;
        }
        finally {
            stopGrid(0);
        }
    }
}
