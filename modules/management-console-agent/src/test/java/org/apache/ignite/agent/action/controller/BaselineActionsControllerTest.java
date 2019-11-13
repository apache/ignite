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

import java.util.List;
import java.util.UUID;
import com.google.common.collect.Lists;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;

/**
 * Baseline actions controller test.
 */
public class BaselineActionsControllerTest extends AbstractActionControllerTest {
    /**
     * Should set the baselineAutoAdjustEnabled cluster property to {@code True}.
     */
    @Test
    public void updateAutoAdjustEnabledToTrue() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("BaselineActions.updateAutoAdjustEnabled")
            .setArgument(true);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && cluster.isBaselineAutoAdjustEnabled());
    }

    /**
     * Should set the baselineAutoAdjustEnabled cluster property to {@code False}.
     */
    @Test
    public void updateAutoAdjustEnabledToFalse() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("BaselineActions.updateAutoAdjustEnabled")
            .setArgument(false);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && !cluster.isBaselineAutoAdjustEnabled());
    }

    /**
     * Should set the baselineAutoAdjustTimeout cluster property to 10_000ms.
     */
    @Test
    public void updateAutoAdjustAwaitingTime() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("BaselineActions.updateAutoAdjustAwaitingTime")
            .setArgument(10_000);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && cluster.baselineAutoAdjustTimeout() == 10_000);
    }

    /**
     * Should set new baseline topology.
     */
    @Test
    public void setBaselineTopology() throws Exception {
        cluster.baselineAutoAdjustEnabled(false);

        IgniteEx ignite_2 = startGrid(2);

        IgniteEx ignite_3 = startGrid(3);

        List<String> ids = Lists.newArrayList(
            cluster.localNode().consistentId().toString(),
            ignite_2.cluster().localNode().consistentId().toString(),
            ignite_3.cluster().localNode().consistentId().toString()
        );

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("BaselineActions.setBaselineTopology")
            .setArgument(ids);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && cluster.currentBaselineTopology().size() == 3);
    }
}
