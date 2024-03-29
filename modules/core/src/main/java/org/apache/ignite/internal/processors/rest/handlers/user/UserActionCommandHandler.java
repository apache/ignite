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

package org.apache.ignite.internal.processors.rest.handlers.user;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.RestUserActionRequest;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.ADD_USER;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.REMOVE_USER;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.UPDATE_USER;

/**
 * User actions handler.
 */
public class UserActionCommandHandler extends GridRestCommandHandlerAdapter {
    /** Commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(ADD_USER, REMOVE_USER, UPDATE_USER);

    /**
     * @param ctx Context.
     */
    public UserActionCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req)
        throws IgniteCheckedException {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling topology REST request: " + req);

        RestUserActionRequest req0 = (RestUserActionRequest)req;

        GridRestCommand cmd = req.command();

        IgniteSecurity security = ctx.security();

        switch (cmd) {
            case ADD_USER:
                security.createUser(req0.user(), req0.password().toCharArray());
                break;

            case REMOVE_USER:
                security.dropUser(req0.user());
                break;

            case UPDATE_USER:
                security.alterUser(req0.user(), req0.password().toCharArray());
                break;
        }

        if (log.isDebugEnabled())
            log.debug("Handled topology REST request [req=" + req + ']');

        return new GridFinishedFuture<>(new GridRestResponse(true));
    }
}
