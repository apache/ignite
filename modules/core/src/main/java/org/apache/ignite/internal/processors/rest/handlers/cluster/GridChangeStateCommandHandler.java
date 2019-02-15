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
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

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
                    ctx.grid().cluster().active(req.active());

                    res.setResponse(req.command().key() + " started");
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
