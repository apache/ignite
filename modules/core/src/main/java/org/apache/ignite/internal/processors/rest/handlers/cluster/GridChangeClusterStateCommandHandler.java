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
import org.apache.ignite.internal.processors.rest.request.GridRestClusterStateRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_SET_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_STATE;

/**
 *
 */
public class GridChangeClusterStateCommandHandler extends GridRestCommandHandlerAdapter {
    /** Commands. */
    private static final Collection<GridRestCommand> COMMANDS = U.sealList(CLUSTER_SET_STATE, CLUSTER_STATE);

    /**
     * @param ctx Context.
     */
    public GridChangeClusterStateCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest restReq) {
        GridRestClusterStateRequest req = (GridRestClusterStateRequest)restReq;

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<>();

        final GridRestResponse res = new GridRestResponse();

        try {
            switch (req.command()) {
                case CLUSTER_STATE:
                    assert req.isReqCurrentMode() : req;

                    res.setResponse(ctx.grid().cluster().state());

                    break;

                default:
                    assert req.state() != null : req;

                    U.log(log, "Received cluster state change request to " + req.state() +
                        " state from client node with ID: " + req.clientId());

                    ctx.state().changeGlobalState(req.state(), req.forceDeactivation(),
                        ctx.cluster().get().forServers().nodes(), false).get();

                    res.setResponse(req.command().key() + " done");

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
