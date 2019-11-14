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

package org.apache.ignite.agent.action;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.agent.utils.AgentUtils.authenticate;

/**
 * Security session registry.
 */
public class SessionRegistry {
    /** Context. */
    private final GridKernalContext ctx;

    /** SessionId-Session map. */
    private final ConcurrentMap<UUID, Session> sesIdToSes = new ConcurrentHashMap<>();

    /** Session time to live. */
    private final long sesTtl;

    /** Interval to invalidate session tokens. */
    private final long sesTokTtl;

    /** Security enabled. */
    private boolean securityEnabled;

    /** Authentication enabled. */
    private boolean authenticationEnabled;

    /**
     * @param ctx Context.
     */
    public SessionRegistry(GridKernalContext ctx) {
        this.ctx = ctx;
        this.securityEnabled = ctx.security().enabled();
        this.authenticationEnabled = ctx.authentication().enabled();

        ManagementConfiguration cfg = ctx.managementConsole().configuration();

        this.sesTtl = cfg.getSecuritySessionTimeout();
        this.sesTokTtl = cfg.getSecuritySessionExpirationTimeout();
    }

    /**
     * @param ses Session.
     */
    public void saveSession(Session ses) {
        sesIdToSes.put(ses.id(), ses);

        scheduleToRemove(ses.id());
    }

    /**
     * @param sesId Session ID.
     * @return Not null session.
     * @throws IgniteAuthenticationException If failed.
     */
    public Session getSession(UUID sesId) throws IgniteCheckedException {
        if (sesId == null)
            throw new IgniteAuthenticationException("Invalid session ID: null");

        Session ses = sesIdToSes.get(sesId);

        if (ses == null)
            throw new IgniteAuthenticationException("Session not found for ID: " + sesId);

        if (!ses.touch() || ses.timedOut(sesTtl)) {
            sesIdToSes.remove(ses.id(), ses);

            if (ctx.security().enabled() && ses.securityContext() != null && ses.securityContext().subject() != null)
                ctx.security().onSessionExpired(ses.securityContext().subject().id());

            return null;
        }

        if (ses.sessionExpired(sesTokTtl)) {
            ses.lastInvalidateTime(U.currentTimeMillis());

            try {
                if (securityEnabled)
                    ses.securityContext(authenticate(ctx.security(), ses));
                else if (authenticationEnabled)
                    ses.authorizationContext(authenticate(ctx.authentication(), ses));

                saveSession(ses);
            }
            catch (IgniteCheckedException ex) {
                sesIdToSes.remove(ses.id());

                throw ex;
            }
        }

        return ses;
    }

    /**
     * @param sesId Session id.
     */
    private void scheduleToRemove(UUID sesId) {
        ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(sesTtl) {
            @Override public void onTimeout() {
                sesIdToSes.computeIfPresent(sesId, (id, ses) -> {
                    if (ses.timedOut(sesTtl))
                        return null;
                    else
                        scheduleToRemove(sesId);

                    return ses;
                });
            }
        });
    }
}
