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

package org.apache.ignite.internal.processors.rest.handlers.datastructures;

import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.DataStructuresRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_DECREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_INCREMENT;

/**
 *
 */
public class DataStructuresCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(
        ATOMIC_INCREMENT,
        ATOMIC_DECREMENT
    );

    /**
     * @param ctx Context.
     */
    public DataStructuresCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert SUPPORTED_COMMANDS.contains(req.command()) : req.command();

        return incrementOrDecrement((DataStructuresRequest)req).chain(
            new CX1<IgniteInternalFuture<?>, GridRestResponse>() {
                @Override public GridRestResponse applyx(IgniteInternalFuture<?> fut) throws IgniteCheckedException {
                    GridRestResponse res = new GridRestResponse();

                    res.setResponse(fut.get());

                    return res;
                }
            }
        );
    }
    /**
     * Handles increment and decrement commands.
     *
     * @param req Request.
     * @return Future of operation result.
     */
    private IgniteInternalFuture<?> incrementOrDecrement(final DataStructuresRequest req) {
        assert req != null;
        assert req.command() == ATOMIC_INCREMENT || req.command() == ATOMIC_DECREMENT : req.command();

        if (req.key() == null) {
            IgniteCheckedException err =
                new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("key"));

            return new GridFinishedFuture(err);
        }
        else if (req.delta() == null) {
            IgniteCheckedException err =
                new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("delta"));

            return new GridFinishedFuture(err);
        }

        return ctx.closure().callLocalSafe(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Long init = req.initial();
                Long delta = req.delta();

                boolean decr = req.command() == ATOMIC_DECREMENT;

                String key = (String)req.key();

                IgniteAtomicLong l = ctx.grid().atomicLong(key, init != null ? init : 0, true);

                return l.addAndGet(decr ? -delta : delta);
            }
        }, false);
    }
}