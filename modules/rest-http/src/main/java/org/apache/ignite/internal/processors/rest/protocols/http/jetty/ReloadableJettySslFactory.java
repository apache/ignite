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
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ssl.ReloadableSslFactory;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.ssl.AbstractSslContextFactory.DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS;

/**
 * TLS factory of the Jetty HTTPS connector. A failed reload leaves the previous keys and trust settings in use.
 * Hot reload follows {@link AbstractSslContextFactory#isHotReloadEnabled()} and
 * {@link AbstractSslContextFactory#getHotReloadBeforeExpiryMs()} of the SSL factory of the REST connector. If that
 * factory is not an {@link AbstractSslContextFactory}, hot reload is enabled and comes
 * {@link AbstractSslContextFactory#DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS} before the certificates expire.
 * <p>
 * Jetty's factory guards its TLS state with an internal lock: it loads the state under this lock, and new connections
 * take the state under it too. Before loading a replacement, Jetty clears the state. So that new connections never see
 * a factory without keys, the reload and, if loading fails, the restore of the previous state run under this lock.
 * Jetty lets callers hold the lock only inside {@link SslContextFactory#unload(Consumer)} and
 * {@link SslContextFactory#reload(Consumer)}, so the work runs inside {@code unload(...)} and ends with {@link SkipUnload}.
 * Jetty selects certificates for SNI host names from the key store of the state, so the restore keeps SNI selection.
 * If the restore fails too, the factory has no state until the next successful reload, and the reload error carries
 * that failure as a suppressed exception.
 * <p>
 * After a failed reload, the factory gets back the context and stores set in {@code jetty.xml}, and the next reload
 * reads its configured sources again. Once the factory has started, its getters return the loaded state rather than
 * these settings. So the adapter cannot keep a context or store that code sets on the running factory: a failed reload
 * drops it.
 */
class ReloadableJettySslFactory implements ReloadableSslFactory {
    /** The adapter is serializable only because {@link Factory} is. Serializing it fails, as Jetty's factory is not serializable. */
    private static final long serialVersionUID = 0L;

    /** */
    private final SslContextFactory factory;

    /** State set in {@code jetty.xml}. The factory loads a {@code null} part from its configured source. */
    private final TlsState configured;

    /** SSL factory of the REST connector with the hot reload settings, or {@code null} to use the defaults. */
    @Nullable private final AbstractSslContextFactory hotReloadSettings;

    /**
     * @param factory TLS factory of the HTTPS connector before startup, when its getters return the state set in {@code jetty.xml}.
     * @param connectorFactory SSL factory of the REST connector.
     */
    ReloadableJettySslFactory(SslContextFactory factory, @Nullable Factory<SSLContext> connectorFactory) {
        assert !factory.isStarted();

        this.factory = factory;

        configured = new TlsState(factory);
        hotReloadSettings = connectorFactory instanceof AbstractSslContextFactory sslFactory ? sslFactory : null;
    }

    /** {@inheritDoc} */
    @Override public SSLContext create() {
        return factory.getSslContext();
    }

    /** {@inheritDoc} */
    @Override public void init() {
        try {
            factory.unload(ignored -> reloadOrRestore());
        }
        catch (SkipUnload skip) {
            if (skip.loadErr != null)
                throw new IgniteException(skip.loadErr);
        }
        catch (IllegalStateException e) {
            throw new IgniteException("Failed to read the TLS state of the HTTPS connector", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isHotReloadEnabled() {
        return hotReloadSettings == null || hotReloadSettings.isHotReloadEnabled();
    }

    /** {@inheritDoc} */
    @Override public long getHotReloadTime() {
        try {
            KeyStore store = factory.getKeyStore();
            long expiry = store == null ? 0 : ReloadableSslFactory.keyChainsExpiry(store);

            long lead = hotReloadSettings == null ? DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS : hotReloadSettings.getHotReloadBeforeExpiryMs();

            return expiry == 0 ? 0 : expiry - lead;
        }
        catch (IllegalStateException | GeneralSecurityException e) {
            throw new IgniteException("Failed to read the certificates of the HTTPS connector", e);
        }
    }

    /**
     * Reloads the factory and, if loading fails, restores the state that new connections used before. Runs under the
     * factory lock inside {@code unload(...)}, and always throws {@link SkipUnload}.
     */
    private void reloadOrRestore() {
        TlsState prev = new TlsState(factory);

        try {
            factory.reload(ignored -> {});
        }
        catch (Throwable e) {
            restore(prev, e);

            throw new SkipUnload(e);
        }

        throw new SkipUnload(null);
    }

    /** Makes the factory load the previous state, then sets back the state from {@code jetty.xml} for the next reload. */
    private void restore(TlsState prev, Throwable loadErr) {
        try {
            factory.reload(prev::applyTo);
        }
        catch (Throwable e) {
            loadErr.addSuppressed(e);
        }
        finally {
            configured.applyTo(factory);
        }
    }

    /** Context and stores that Jetty uses together for new connections. */
    private record TlsState(SSLContext ctx, KeyStore keyStore, KeyStore trustStore) {
        /** */
        TlsState(SslContextFactory factory) {
            this(factory.getSslContext(), factory.getKeyStore(), factory.getTrustStore());
        }

        /** Sets this state on the factory, which loads it on the next reload. */
        private void applyTo(SslContextFactory factory) {
            factory.setSslContext(ctx);
            factory.setKeyStore(keyStore);
            factory.setTrustStore(trustStore);
        }
    }

    /**
     * Ends the outer {@code unload(...)} before it unloads the factory. It does not bring back the state that a failed
     * reload inside has already cleared: {@code restore(...)} does that.
     */
    private static class SkipUnload extends RuntimeException {
        /** */
        private static final long serialVersionUID = 0L;

        /** Load failure, or {@code null} if the factory has the replacement. */
        @Nullable private final Throwable loadErr;

        /** */
        private SkipUnload(@Nullable Throwable loadErr) {
            super(null, null, false, false);

            this.loadErr = loadErr;
        }
    }
}
