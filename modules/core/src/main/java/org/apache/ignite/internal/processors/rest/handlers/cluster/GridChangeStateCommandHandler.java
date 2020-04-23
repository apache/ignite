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

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestChangeStateRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_CURRENT_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_DEACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_INACTIVE;

/**
 *
 */
public class GridChangeStateCommandHandler extends GridRestCommandHandlerAdapter {
    /** Commands. */
    private static final Collection<GridRestCommand> commands =
        U.sealList(CLUSTER_ACTIVATE, CLUSTER_DEACTIVATE, CLUSTER_CURRENT_STATE, CLUSTER_ACTIVE, CLUSTER_INACTIVE);

    /**
     * @param ctx Context.
     */
    public GridChangeStateCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return commands;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest restRest) {
        GridRestChangeStateRequest req = (GridRestChangeStateRequest)restRest;

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<>();

        final GridRestResponse res = new GridRestResponse();

        try {
            switch (req.command()) {
                case CLUSTER_CURRENT_STATE:
                    Boolean currentState = ctx.state().publicApiActiveState(false);

                    res.setResponse(currentState);
                    break;
                case CLUSTER_ACTIVE:
                case CLUSTER_INACTIVE:
                    log.warning(req.command().key() + " is deprecated. Use newer commands.");
                default:
                    ctx.state().changeGlobalState(req.active() ? ACTIVE : INACTIVE, req.forceDeactivation(),
                        ctx.cluster().get().forServers().nodes(), false).get();

                    res.setResponse(req.command().key() + " started");
                    break;
            }

            fut.onDone(res);
        }
        catch (Exception e) {
            res.setError(errorMessage(e));

            fut.onDone(res);
        }
        return fut;
    }
}
