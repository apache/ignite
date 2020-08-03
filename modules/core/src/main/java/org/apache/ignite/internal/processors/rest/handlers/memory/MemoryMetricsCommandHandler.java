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

package org.apache.ignite.internal.processors.rest.handlers.memory;

import java.util.Collection;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.DATA_REGION_METRICS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.DATA_STORAGE_METRICS;

/**
 * Command handler for {@link DataStorageMetrics} or a collection of {@link DataRegionMetrics}
 */
public class MemoryMetricsCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * Supported commands.
     */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(
        DATA_STORAGE_METRICS, DATA_REGION_METRICS);

    /**
     * @param ctx Context.
     */
    public MemoryMetricsCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling " + req.command().key() + " REST request: " + req);

        GridRestCommand cmd = req.command();

        switch (cmd) {
            case DATA_REGION_METRICS:
                return new GridFinishedFuture<>(new GridRestResponse(ctx.grid().dataRegionMetrics()));

            case DATA_STORAGE_METRICS:
                if (ctx.config().getDataStorageConfiguration().isMetricsEnabled())
                    return new GridFinishedFuture<>(new GridRestResponse(ctx.grid().dataStorageMetrics()));
                else
                    return new GridFinishedFuture<>(new GridRestResponse("Storage metrics are not enabled"));

            default:
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED, "Unknown command"));
        }
    }
}
