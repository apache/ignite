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

package org.apache.ignite.internal.processors.rest.handlers.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestBaselineRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.BASELINE_ADD;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.BASELINE_CURRENT_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.BASELINE_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.BASELINE_SET;

/**
 *
 */
public class GridBaselineCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(BASELINE_CURRENT_STATE,
        BASELINE_SET, BASELINE_ADD, BASELINE_REMOVE);

    /**
     * @param ctx Context.
     */
    public GridBaselineCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());
        assert req instanceof GridRestBaselineRequest : "Invalid type of baseline request.";

        if (log.isDebugEnabled())
            log.debug("Handling baseline REST request: " + req);

        GridRestBaselineRequest req0 = (GridRestBaselineRequest)req;
        
        try {
            IgniteClusterEx cluster = ctx.grid().cluster();

            List<Object> consistentIds = req0.consistentIds();

            switch (req0.command()) {
                case BASELINE_CURRENT_STATE: {
                    // No-op.

                    break;
                }

                case BASELINE_SET: {
                    Long topVer = req0.topologyVersion();

                    if (topVer == null && consistentIds == null)
                        throw new IgniteCheckedException("Failed to handle request (either topVer or consistentIds should be specified).");

                    if (topVer != null)
                        cluster.setBaselineTopology(topVer);
                    else
                        cluster.setBaselineTopology(filterServerNodesByConsId(consistentIds));

                    break;
                }

                case BASELINE_ADD: {
                    if (consistentIds == null)
                        throw new IgniteCheckedException(missingParameter("consistentIds"));

                    Set<BaselineNode> baselineTop = new HashSet<>(currentBaseLine());

                    baselineTop.addAll(filterServerNodesByConsId(consistentIds));

                    cluster.setBaselineTopology(baselineTop);

                    break;
                }

                case BASELINE_REMOVE: {
                    if (consistentIds == null)
                        throw new IgniteCheckedException(missingParameter("consistentIds"));

                    Collection<BaselineNode> baseline = currentBaseLine();

                    Set<BaselineNode> baselineTop = new HashSet<>(baseline);

                    baselineTop.removeAll(filterNodesByConsId(baseline, consistentIds));

                    cluster.setBaselineTopology(baselineTop);

                    break;
                }

                default:
                    assert false : "Invalid command for baseline handler: " + req;
            }

            return new GridFinishedFuture<>(new GridRestResponse(currentState()));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
        finally {
            if (log.isDebugEnabled())
                log.debug("Handled baseline REST request: " + req);
        }
    }

    /**
     * @return Current baseline.
     */
    private Collection<BaselineNode> currentBaseLine() {
        Collection<BaselineNode> baselineNodes = ctx.grid().cluster().currentBaselineTopology();

        return baselineNodes != null ? baselineNodes : Collections.emptyList();
    }

    /**
     * Collect baseline topology command result.
     *
     * @return Baseline descriptor.
     */
    private GridBaselineCommandResponse currentState() {
        IgniteClusterEx cluster = ctx.grid().cluster();

        Collection<? extends BaselineNode> srvrs = cluster.forServers().nodes();

        return new GridBaselineCommandResponse(cluster.active(), cluster.topologyVersion(), currentBaseLine(), srvrs);
    }

    /**
     * Filter passed nodes by consistent IDs.
     *
     * @param nodes Collection of nodes.
     * @param consistentIds Collection of consistent IDs.
     * @throws IllegalStateException In case of some consistent ID not found in nodes collection.
     */
    private Collection<BaselineNode> filterNodesByConsId(Collection<? extends BaselineNode> nodes, List<Object> consistentIds) {
        Map<Object, BaselineNode> nodeMap =
            nodes.stream().collect(toMap(n -> n.consistentId().toString(), identity()));

        Collection<BaselineNode> filtered = new ArrayList<>(consistentIds.size());

        for (Object consistentId : consistentIds) {
            BaselineNode node = nodeMap.get(consistentId);

            if (node == null)
                throw new IllegalStateException("Node not found for consistent ID: " + consistentId);

            filtered.add(node);
        }

        return filtered;
    }

    /**
     * Filter server nodes by consistent IDs.
     *
     * @param consistentIds Collection of consistent IDs to add.
     * @throws IllegalStateException In case of some consistent ID not found in nodes collection.
     */
    private Collection<BaselineNode> filterServerNodesByConsId(List<Object> consistentIds) {
        return filterNodesByConsId(ctx.grid().cluster().forServers().nodes(), consistentIds);
    }
}
