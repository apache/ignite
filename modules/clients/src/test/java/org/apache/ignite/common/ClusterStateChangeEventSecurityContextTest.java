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

package org.apache.ignite.common;

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_STATE_CHANGE_STARTED;

/** Tests that security information specified in cluster state change event belongs to the operation initiator. */
public class ClusterStateChangeEventSecurityContextTest extends AbstractEventSecurityContextTest {
    /** {@inheritDoc} */
    @Override protected int[] eventTypes() {
        return new int[] {EVT_CLUSTER_STATE_CHANGE_STARTED};
    }

    /** */
    @Test
    public void testClusterStateChangeStartedEvent() throws Exception {
        startGridAllowAll("crd");
        startGridAllowAll("srv");
        startClientAllowAll("cli");

        for (Ignite grid : G.allGrids())
            checkEvent(state -> grid.cluster().state(state), grid.name());

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(THIN_CLIENT_LOGIN)
            .setUserPassword("");

        try (IgniteClient cli = Ignition.startClient(cfg)) {
            checkEvent(state -> cli.cluster().state(state), THIN_CLIENT_LOGIN);
        }
    }

    /** */
    private void checkEvent(Consumer<ClusterState> call, String initiator) throws Exception {
        for (ClusterState state : new ClusterState[]{INACTIVE, ACTIVE_READ_ONLY, ACTIVE})
            checkEvents(() -> call.accept(state), singletonList(EVT_CLUSTER_STATE_CHANGE_STARTED), initiator);
    }
}
