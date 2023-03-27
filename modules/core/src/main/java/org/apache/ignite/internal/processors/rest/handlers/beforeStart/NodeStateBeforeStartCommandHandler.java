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

package org.apache.ignite.internal.processors.rest.handlers.beforeStart;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestNodeStateBeforeStartRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestWarmUpRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Command handler for managing state of a node before it starts and getting information about it.
 */
public class NodeStateBeforeStartCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * Construecor.
     *
     * @param ctx Kernal context.
     */
    public NodeStateBeforeStartCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return Arrays.asList(GridRestCommand.NODE_STATE_BEFORE_START, GridRestCommand.WARM_UP);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        GridRestNodeStateBeforeStartRequest restReq = (GridRestNodeStateBeforeStartRequest)req;

        if (log.isDebugEnabled())
            log.debug("Handling REST request: " + req);

        try {
            if (restReq instanceof GridRestWarmUpRequest) {
                GridRestWarmUpRequest warmUpReq = (GridRestWarmUpRequest)restReq;

                if (warmUpReq.stopWarmUp())
                    ctx.cache().stopWarmUp();
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(e);
        }

        return new GridFinishedFuture<>(new GridRestResponse());
    }
}
