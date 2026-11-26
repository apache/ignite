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
import org.apache.ignite.ssl.jetty.AutoReloadJettySslFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.ssl.AbstractSslContextFactory.DFLT_AUTO_RELOAD_BEFORE_EXPIRY_MS;

/**
 * Adapter through which the SSL factory registry reloads the TLS factory of the Jetty HTTPS connector. A failed reload
 * leaves the previous keys and trust settings in use.
 * Automatic reload follows the settings of the Jetty factory if it implements {@link AutoReloadJettySslFactory}. Otherwise
 * automatic reload is enabled with the lead time {@link AbstractSslContextFactory#DFLT_AUTO_RELOAD_BEFORE_EXPIRY_MS}.
 * <p>
 * Jetty's factory guards its TLS state with an internal lock: it loads the state under this lock, and new connections
 * take the state under it too. Before loading a replacement, Jetty clears the state. So that new connections never see
 * a factory without keys, the reload and, if loading fails, the restore of the previous state run under this lock.
 * Jetty lets callers hold the lock only inside {@link SslContextFactory#unload(Consumer)} and
 * {@link SslContextFactory#reload(Consumer)}, so the work runs inside {@code unload(...)} and ends with {@link SkipUnload}.
 * Jetty selects certificates for SNI host names from the key store of the state, so the restore keeps SNI selection.
 * If the restore fails too, the factory has no state until the next successful reload, and the reload error carries
 * that failure as a suppressed exception. A failed reload outside Ignite, such as one by Jetty's {@code KeyStoreScanner},
 * can also leave the factory without state. Such a factory is due for automatic reload right away, and the reload has nothing
 * to restore: it loads the configured sources.
 * <p>
 * After a failed reload, the factory gets back the context and stores set in {@code jetty.xml}, and the next reload
 * reads its configured sources again. Once the factory has started, its getters return the loaded state rather than
 * these settings. So the adapter cannot keep a context or store that code sets on the running factory: a failed reload
 * drops it.
 */
class JettySslFactoryAdapter implements ReloadableSslFactory {
    /** The adapter is serializable only because {@link Factory} is. Serializing it fails, as Jetty's factory is not serializable. */
    private static final long serialVersionUID = 0L;

    /** */
    private final SslContextFactory factory;

    /** State set in {@code jetty.xml}. The factory loads a {@code null} part from its configured source. */
    private final TlsState configured;

    /**
     * @param factory TLS factory of the HTTPS connector before startup, when its getters return the state set in {@code jetty.xml}.
     */
    JettySslFactoryAdapter(SslContextFactory factory) {
        assert !factory.isStarted();

        this.factory = factory;

        configured = new TlsState(factory);
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
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoReloadEnabled() {
        return !(factory instanceof AutoReloadJettySslFactory reloadable) || reloadable.isAutoReloadEnabled();
    }

    /** {@inheritDoc} */
    @Override public long autoReloadTime() {
        TlsState state = currentState();

        if (state == null)
            return System.currentTimeMillis();

        try {
            KeyStore store = state.keyStore();
            long expiry = store == null ? 0 : ReloadableSslFactory.keyChainsExpiry(store);

            return expiry == 0 ? 0 : expiry - autoReloadBeforeExpiryMs();
        }
        catch (GeneralSecurityException e) {
            throw new IgniteException("Failed to read the certificates of the HTTPS connector", e);
        }
    }

    /** @return How long before the certificates expire automatic reload becomes due, in milliseconds. */
    private long autoReloadBeforeExpiryMs() {
        return factory instanceof AutoReloadJettySslFactory reloadable
            ? reloadable.getAutoReloadBeforeExpiryMs()
            : DFLT_AUTO_RELOAD_BEFORE_EXPIRY_MS;
    }

    /**
     * Reloads the factory and, if loading fails, restores the state that new connections used before, if there was one.
     * Runs under the factory lock inside {@code unload(...)}, and always throws {@link SkipUnload}.
     */
    private void reloadOrRestore() {
        TlsState prev = currentState();

        try {
            factory.reload(ignored -> {});
        }
        catch (Throwable e) {
            if (prev != null)
                restore(prev, e);

            throw new SkipUnload(e);
        }

        throw new SkipUnload(null);
    }

    /** @return State that new connections use, or {@code null} if a failed reload has left the factory without state. */
    @Nullable private TlsState currentState() {
        try {
            return new TlsState(factory);
        }
        catch (IllegalStateException noState) {
            return null;
        }
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
