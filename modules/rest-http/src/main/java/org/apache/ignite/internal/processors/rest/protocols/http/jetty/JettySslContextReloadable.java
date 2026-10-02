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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.ssl.X509;
import org.jetbrains.annotations.Nullable;

/**
 * Certificate reload for the Jetty connector serving HTTP REST.
 * <p>
 * Jetty rebuilds the context in place and has no rollback of its own: once its own reload has failed, the connector
 * serves no TLS at all until the next successful one. The stores are therefore verified on a throw-away factory
 * first, and the context in use is pinned back if the rebuild fails regardless.
 */
public class JettySslContextReloadable implements SslContextReloadable {
    /** Attempt whose check this connector passed, {@code null} when there is nothing prepared. */
    private volatile UUID prepared;

    /** SSL factory of the running connector. */
    private final SslContextFactory.Server sslCtxFactory;

    /**
     * @param sslCtxFactory SSL factory of the running connector.
     */
    public JettySslContextReloadable(SslContextFactory.Server sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> users() {
        return Collections.singleton(SslContextReloadable.HTTP_REST);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Jetty rebuilds its context in place, so nothing can be held aside here: this checks the stores and remembers
     * that they were readable. The connector is therefore the one place a rotation can still stop half way, which
     * is bounded — it takes no part in the traffic between nodes.
     */
    @Override public boolean prepare(UUID token) throws IgniteCheckedException {
        // Dropped first, so that a check that throws leaves nothing behind that any attempt could still apply.
        discard();

        boolean rebuildable = rebuildable();

        if (rebuildable)
            verifyStores();

        // Remembered even when there is nothing to re-read, so that the second phase reports a connector handed a
        // ready-made context as having nothing to apply, rather than as one this attempt never reached.
        prepared = token;

        return rebuildable;
    }

    /** {@inheritDoc} */
    @Override public Commit commit(UUID token) {
        UUID prepared0 = prepared;

        if (prepared0 == null || !prepared0.equals(token))
            return Commit.NOT_PREPARED;

        discard();

        try {
            return reload() ? Commit.APPLIED : Commit.NOTHING_TO_APPLY;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void discard() {
        prepared = null;
    }

    /**
     * @return {@code True} if the connector now serves a rebuilt context.
     * @throws IgniteCheckedException If it could not be rebuilt. The context in use is kept.
     */
    private boolean reload() throws IgniteCheckedException {
        if (!rebuildable())
            return false;

        verifyStores();

        SSLContext cur = sslCtxFactory.getSslContext();

        try {
            // Dropping the pinned context makes Jetty read the stores again, which also recovers the factory if an
            // earlier attempt had to pin one.
            sslCtxFactory.reload(factory -> factory.setSslContext(null));
        }
        catch (Exception e) {
            pin(cur);

            throw new IgniteCheckedException(e);
        }

        return sslCtxFactory.getSslContext() != cur;
    }

    /** {@inheritDoc} */
    @Override public @Nullable X509Certificate servedCertificate() {
        Set<String> aliases = sslCtxFactory.getAliases();

        if (aliases.isEmpty())
            return null;

        X509 x509 = sslCtxFactory.getX509(aliases.iterator().next());

        return x509 == null ? null : x509.getCertificate();
    }

    /**
     * @return {@code True} if the connector was configured with stores on disk. A connector handed a ready-made
     *      context by the Jetty configuration has nothing to re-read.
     */
    private boolean rebuildable() {
        return sslCtxFactory.getKeyStorePath() != null;
    }

    /**
     * Loads the configured stores into a throw-away factory, so that a broken one is reported before it can reach
     * the connector.
     *
     * @throws IgniteCheckedException If the stores could not be loaded.
     */
    private void verifyStores() throws IgniteCheckedException {
        SslContextFactory.Server probe = new SslContextFactory.Server();

        probe.setKeyStorePath(sslCtxFactory.getKeyStorePath());
        probe.setKeyStoreType(sslCtxFactory.getKeyStoreType());
        probe.setKeyStoreProvider(sslCtxFactory.getKeyStoreProvider());
        probe.setKeyStorePassword(sslCtxFactory.getKeyStorePassword());
        probe.setKeyManagerPassword(sslCtxFactory.getKeyManagerPassword());

        try {
            probe.start();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            try {
                probe.stop();
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /**
     * @param ctx Context to keep serving after a failed rebuild.
     */
    private void pin(SSLContext ctx) {
        try {
            sslCtxFactory.reload(factory -> factory.setSslContext(ctx));
        }
        catch (Exception ignored) {
            // Nothing better is available: the connector is already unable to serve TLS.
        }
    }
}
