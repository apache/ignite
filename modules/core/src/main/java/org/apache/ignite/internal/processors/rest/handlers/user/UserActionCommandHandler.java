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

package org.apache.ignite.internal.processors.rest.handlers.user;

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.RestUserActionRequest;
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
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling topology REST request: " + req);

        RestUserActionRequest req0 = (RestUserActionRequest)req;

        try {
            GridRestCommand cmd = req.command();

            IgniteAuthenticationProcessor authentication = ctx.authentication();

            AuthorizationContext.context(req.authorizationContext());

            switch (cmd) {
                case ADD_USER:
                    authentication.addUser(req0.user(), req0.password());
                    break;

                case REMOVE_USER:
                    authentication.removeUser(req0.user());
                    break;

                case UPDATE_USER:
                    authentication.updateUser(req0.user(), req0.password());
                    break;
            }

            if (log.isDebugEnabled())
                log.debug("Handled topology REST request [req=" + req + ']');

            return new GridFinishedFuture<>(new GridRestResponse(true));
        }
        catch (Throwable e) {
            log.error("Failed to handle REST request [req=" + req + ']', e);

            return new GridFinishedFuture<>(e);
        }
    }
}
