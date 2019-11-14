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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.agent.action.annotation.ActionController;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.security.SecurityPermission;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.agent.utils.AgentUtils.authorizeIfNeeded;
import static org.apache.ignite.agent.utils.AgentUtils.fromNullableCollection;

/**
 * Baseline actions controller.
 */
@ActionController("BaselineActions")
public class BaselineActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public BaselineActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param isAutoAdjustEnabled Is auto adjust enabled.
     */
    public void updateAutoAdjustEnabled(boolean isAutoAdjustEnabled) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().baselineAutoAdjustEnabled(isAutoAdjustEnabled);
    }

    /**
     * @param awaitingTime Awaiting time in ms.
     */
    public void updateAutoAdjustAwaitingTime(long awaitingTime) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().baselineAutoAdjustTimeout(awaitingTime);
    }

    /**
     * @param consIds Node consistent ids.
     */
    public void setBaselineTopology(Collection<String> consIds) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().setBaselineTopology(baselineNodesForIds(consIds));
    }

    /**
     * @param consIds Node consistent ids.
     */
    private Collection<BaselineNode> baselineNodesForIds(Collection<String> consIds) {
        Map<String, BaselineNode> srvrs = currentServers();

        Map<String, BaselineNode> baseline = currentBaseLine();

        Collection<BaselineNode> baselineTop = new ArrayList<>();

        for (String consistentId : consIds) {
            if (srvrs.get(consistentId) != null)
                baselineTop.add(srvrs.get(consistentId));
            else if (baseline.get(consistentId) != null)
                baselineTop.add(baseline.get(consistentId));
        }

        return baselineTop;
    }

    /**
     * @return Current baseline.
     */
    private Map<String, BaselineNode> currentBaseLine() {
        return fromNullableCollection(ctx.grid().cluster().currentBaselineTopology())
            .collect(toMap(n -> n.consistentId().toString(), identity()));
    }

    /**
     * @return Current server nodes.
     */
    private Map<String, BaselineNode> currentServers() {
        return ctx.grid().cluster().forServers().nodes().stream()
            .collect(toMap(n -> n.consistentId().toString(), identity()));
    }
}
