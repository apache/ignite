/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action.controller;

import java.util.UUID;
import org.apache.ignite.agent.dto.action.Request;
import org.junit.Test;

import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;

/**
 * Cluster actions controller test.
 */
public class ClusterActionsControllerTest extends AbstractActionControllerTest {
    /**
     * Should activate cluster.
     */
    @Test
    public void activateCluster() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ClusterActions.activate");

        executeAction(req, (r) -> r.getStatus() == COMPLETED && cluster.active());
    }

    /**
     * Should deactivate cluster.
     */
    @Test
    public void deactivateCluster() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ClusterActions.deactivate");

        executeAction(req, (r) -> r.getStatus() == COMPLETED && !cluster.active());
    }
}
