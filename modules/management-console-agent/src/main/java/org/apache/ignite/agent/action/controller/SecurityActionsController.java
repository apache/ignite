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

package org.apache.ignite.agent.action.controller;

import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.ManagementConsoleProcessor;
import org.apache.ignite.agent.action.Session;
import org.apache.ignite.agent.action.SessionRegistry;
import org.apache.ignite.agent.action.annotation.ActionController;
import org.apache.ignite.agent.dto.action.AuthenticateCredentials;
import org.apache.ignite.agent.utils.AgentUtils;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Controller for security actions.
 */
@ActionController("SecurityActions")
public class SecurityActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /** Registry. */
    private final SessionRegistry registry;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public SecurityActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(SecurityActionsController.class);
        this.registry = ((ManagementConsoleProcessor)ctx.managementConsole()).sessionRegistry();
    }

    /**
     * @param reqCreds Request credentials.
     * @return Completable feature with token.
     */
    public String authenticate(AuthenticateCredentials reqCreds) throws IgniteCheckedException {
        Session ses = authenticate0(reqCreds);

        registry.saveSession(ses);

        if (log.isDebugEnabled())
            log.debug("Session ID was generated for request: " + ses.id());

        return ses.id().toString();
    }

    /**
     * Authenticates remote client.
     *
     * @return Authentication subject context.
     * @throws IgniteCheckedException If authentication failed.
     */
    private Session authenticate0(AuthenticateCredentials reqCreds) throws IgniteCheckedException {
        boolean securityEnabled = ctx.security().enabled();

        boolean authenticationEnabled = ctx.authentication().enabled();

        if (reqCreds.getCredentials() == null)
            throw new IgniteAuthenticationException("Authentication failed, credentials not found");

        Session ses = Session.random();

        ses.address(reqCreds.getAddress());
        ses.credentials(reqCreds.getCredentials());

        if (securityEnabled) {
            ses.lastInvalidateTime(U.currentTimeMillis());
            ses.securityContext(AgentUtils.authenticate(ctx.security(), ses));
        }
        else if (authenticationEnabled)
            ses.authorizationContext(AgentUtils.authenticate(ctx.authentication(), ses));

        return ses;
    }
}
