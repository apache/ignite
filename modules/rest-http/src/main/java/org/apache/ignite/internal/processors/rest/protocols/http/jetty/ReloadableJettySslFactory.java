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

import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ssl.HotReloadTime;
import org.apache.ignite.internal.processors.ssl.ReloadableSslFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * TLS factory of the Jetty HTTPS connector. A failed reload leaves the previous keys and trust settings in use.
 * <p>
 * Jetty clears its active context before loading the replacement. Holding its lock through reload and recovery lets
 * new connections use the previous context if loading fails. Jetty uses the accompanying key store for SNI.
 * If reinstalling the previous context fails too, the factory has no context until the next successful reload, and the
 * reload error carries that failure as a suppressed exception.
 */
class ReloadableJettySslFactory implements ReloadableSslFactory {
    /** */
    private final SslContextFactory factory;

    /** Objects supplied directly to Jetty. A null store lets the factory read its configured source. */
    private final TlsMaterial configured;

    /** @param factory TLS factory of the HTTPS connector before startup, when its getters expose the configured objects. */
    ReloadableJettySslFactory(SslContextFactory factory) {
        this.factory = factory;

        configured = new TlsMaterial(factory);
    }

    /** {@inheritDoc} */
    @Override public void init() {
        try {
            factory.unload(this::reloadLocked);
        }
        catch (ReloadDone done) {
            if (done.err != null)
                throw new IgniteException(done.err);
        }
    }

    /** {@inheritDoc} */
    @Override public long getHotReloadTime() {
        try {
            KeyStore store = factory.getKeyStore();

            return store == null ? 0 : HotReloadTime.of(store);
        }
        catch (IllegalStateException | GeneralSecurityException e) {
            throw new IgniteException("Failed to read the certificates of the HTTPS connector", e);
        }
    }

    /**
     * Reloads the factory while {@link SslContextFactory#unload(Consumer)} holds its lock.
     * Always throws {@link ReloadDone}, so that {@code unload(...)} does not unload the factory.
     */
    private void reloadLocked(SslContextFactory f) {
        TlsMaterial prev = new TlsMaterial(f);

        try {
            f.reload(ignored -> {});
        }
        catch (Exception e) {
            restore(f, prev, e);

            throw new ReloadDone(e);
        }

        throw new ReloadDone(null);
    }

    /** Reinstalls the working context, then restores the settings Jetty will use on the next reload. */
    private void restore(SslContextFactory f, TlsMaterial prev, Exception err) {
        try {
            f.reload(prev::applyTo);
        }
        catch (Exception e) {
            err.addSuppressed(e);
        }
        finally {
            configured.applyTo(f);
        }
    }

    /** The context and stores Jetty uses together for new connections. */
    private record TlsMaterial(SSLContext ctx, KeyStore keyStore, KeyStore trustStore) {
        /** */
        TlsMaterial(SslContextFactory factory) {
            this(factory.getSslContext(), factory.getKeyStore(), factory.getTrustStore());
        }

        /** */
        private void applyTo(SslContextFactory factory) {
            factory.setSslContext(ctx);
            factory.setKeyStore(keyStore);
            factory.setTrustStore(trustStore);
        }
    }

    /** Ends {@code unload(...)} without unloading the factory. */
    private static class ReloadDone extends RuntimeException {
        /** */
        private static final long serialVersionUID = 0L;

        /** Load failure, or {@code null} if the factory has the replacement. */
        @Nullable private final Exception err;

        /** */
        private ReloadDone(@Nullable Exception err) {
            super(null, null, false, false);

            this.err = err;
        }
    }
}
