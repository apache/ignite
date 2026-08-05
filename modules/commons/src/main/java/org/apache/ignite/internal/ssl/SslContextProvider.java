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

package org.apache.ignite.internal.ssl;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Owns the SSL context built out of one configured factory, and hands it to everything that factory was given to.
 * <p>
 * A transport asks for the context whenever it opens a connection, so replacing the context here is what puts
 * rotated certificates in use: connections opened afterwards get the new one, established ones are not touched.
 * One provider stands for one factory, however many transports share it, so a rotation cannot leave them on
 * certificates read at different moments.
 */
public class SslContextProvider implements SslContextReloadable {
    /** Builds a context out of whatever the configured stores hold at the moment of the call. */
    private final Factory<SSLContext> factory;

    /** Transports served, sorted, as the reload command reports them. */
    private final Set<String> users = new ConcurrentSkipListSet<>();

    /** Whether any user connects nodes to each other, which is what makes a context worth checking before use. */
    private volatile boolean interNode;

    /** Context in use. */
    private volatile SSLContext ctx;

    /** What one attempt built and checked, waiting to be put in use; {@code null} when there is nothing prepared. */
    private volatile Staged staged;

    /**
     * @param factory Factory to build the context with.
     */
    public SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        ctx = factory.create();
    }

    /**
     * @return Context to open the next connection with.
     */
    public SSLContext context() {
        return ctx;
    }

    /**
     * @param user Transport the context is handed to.
     * @param interNode Whether that transport connects nodes to each other.
     */
    public void addUser(String user, boolean interNode) {
        users.add(user);

        if (interNode)
            this.interNode = true;
    }

    /**
     * @return Transports this provider serves.
     */
    public Collection<String> users() {
        return Collections.unmodifiableCollection(users);
    }

    /**
     * Builds the certificates on disk and puts them in use in one step, for callers that have no operator to show
     * them to first.
     *
     * @return {@code True} if new certificates were put in use.
     * @throws IgniteCheckedException If they could not be built or would not be accepted.
     */
    public boolean reload() throws IgniteCheckedException {
        UUID token = UUID.randomUUID();

        return prepare(token) && commit(token) == Commit.APPLIED;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean prepare(UUID token) throws IgniteCheckedException {
        // Dropped first, so that a rebuild that throws leaves nothing behind that any attempt could still apply.
        discard();

        staged = rebuild(token);

        return staged.ctx != null;
    }

    /** {@inheritDoc} */
    @Override public synchronized Commit commit(UUID token) {
        Staged staged0 = staged;

        // Applying what another attempt prepared would put certificates in use that this one never showed to the
        // operator, so a token that does not match counts as nothing prepared here.
        if (staged0 == null || !staged0.token.equals(token))
            return Commit.NOT_PREPARED;

        discard();

        if (staged0.ctx == null)
            return Commit.NOTHING_TO_APPLY;

        ctx = staged0.ctx;

        return Commit.APPLIED;
    }

    /** {@inheritDoc} */
    @Override public synchronized void discard() {
        staged = null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable X509Certificate servedCertificate() {
        Staged staged0 = staged;

        // Once something is prepared, this is the certificate the node is about to serve. That is what an operator
        // has to see before confirming: what is in use now is what they are replacing.
        if (staged0 != null && staged0.ctx != null)
            return staged0.cert;

        if (!interNode)
            return null;

        try {
            return SslContextValidator.validateInterNode(ctx);
        }
        catch (SSLException ignored) {
            // The certificate is reported next to what was reloaded, so a context that cannot tell simply says
            // nothing rather than failing the command.
            return null;
        }
    }

    /**
     * @param token Attempt to build for.
     * @return Context built from the stores as they are now, holding no context if the factory handed back the one
     *      already in use and there is therefore nothing to put in use.
     * @throws IgniteCheckedException If the context could not be built, or an inter-node transport would refuse it.
     */
    private Staged rebuild(UUID token) throws IgniteCheckedException {
        try {
            SSLContext rebuilt = factory.create();

            if (rebuilt == ctx)
                return new Staged(token, null, null);

            // The check hands back the certificate the rebuilt context presents, which is what the report names.
            return new Staged(token, rebuilt, interNode ? SslContextValidator.validateInterNode(rebuilt) : null);
        }
        catch (SSLException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Result of one attempt to prepare, kept as a whole so that a commit cannot take the token of one attempt
     * together with the context of another.
     */
    private static class Staged {
        /** Attempt this was built for. */
        private final UUID token;

        /** Context to put in use, {@code null} when the attempt found nothing to apply. */
        private final SSLContext ctx;

        /** Certificate that context presents, {@code null} if it cannot be told without a peer. */
        private final X509Certificate cert;

        /**
         * @param token Attempt this was built for.
         * @param ctx Context to put in use, {@code null} if there is nothing to apply.
         * @param cert Certificate that context presents, {@code null} if it cannot be told without a peer.
         */
        private Staged(UUID token, @Nullable SSLContext ctx, @Nullable X509Certificate cert) {
            this.token = token;
            this.ctx = ctx;
            this.cert = cert;
        }
    }
}
