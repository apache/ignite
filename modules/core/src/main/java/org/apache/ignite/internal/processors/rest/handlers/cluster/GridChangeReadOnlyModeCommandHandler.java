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

package org.apache.ignite.internal.processors.rest.handlers.cluster;

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestReadOnlyChangeModeRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_CURRENT_READ_ONLY_MODE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_READ_ONLY_DISABLE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_READ_ONLY_ENABLE;

/**
 *
 */
public class GridChangeReadOnlyModeCommandHandler extends GridRestCommandHandlerAdapter {
    /** Commands. */
    private static final Collection<GridRestCommand> COMMANDS =
        U.sealList(CLUSTER_CURRENT_READ_ONLY_MODE, CLUSTER_READ_ONLY_DISABLE, CLUSTER_READ_ONLY_ENABLE);

    /**
     * @param ctx Context.
     */
    public GridChangeReadOnlyModeCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest restReq) {
        GridRestReadOnlyChangeModeRequest req = (GridRestReadOnlyChangeModeRequest)restReq;

        final GridFutureAdapter<GridRestResponse> fut = new GridFutureAdapter<>();

        final GridRestResponse res = new GridRestResponse();

        try {
            switch (req.command()) {
                case CLUSTER_CURRENT_READ_ONLY_MODE:
                    res.setResponse(ctx.grid().cluster().readOnly());

                    break;

                default:
                    if (req.readOnly())
                        U.log(log, "Received enable read-only mode request from client node with ID: " + req.clientId());
                    else
                        U.log(log, "Received disable read-only mode request from client node with ID: " + req.clientId());

                    ctx.grid().cluster().readOnly(req.readOnly());

                    res.setResponse(req.command().key() + " done");

                    break;
            }

            fut.onDone(res);
        }
        catch (Exception e) {
            SB sb = new SB();

            sb.a(e.getMessage()).a("\n").a("suppressed: \n");

            for (Throwable t : X.getSuppressedList(e))
                sb.a(t.getMessage()).a("\n");

            res.setError(sb.toString());

            fut.onDone(res);
        }
        return fut;
    }
}
