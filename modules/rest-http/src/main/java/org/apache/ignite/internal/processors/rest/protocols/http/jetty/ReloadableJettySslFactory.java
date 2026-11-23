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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ssl.HotReloadTime;
import org.apache.ignite.internal.processors.ssl.ReloadableSslFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * TLS factory of the Jetty HTTPS connector. A failed reload leaves the previous keys and trust settings in use.
 * <p>
 * Before Jetty loads the replacement, it clears the TLS state of its factory, including the certificates that it selects
 * by SNI host name. New connections take that state under the factory's lock. Jetty lets callers hold this lock only
 * inside {@link SslContextFactory#unload(Consumer)} and {@link SslContextFactory#reload(Consumer)}, so the reload runs
 * inside {@code unload(...)}. If loading fails, the factory loads its previous key and trust stores from memory before
 * the lock is released. New connections wait and then get either the replacement or the previous keys. The next reload
 * reads the factory's sources again, so stores that were set on the factory directly are not kept.
 */
class ReloadableJettySslFactory implements ReloadableSslFactory {
    /** */
    private final SslContextFactory factory;

    /** @param factory Started TLS factory of the HTTPS connector. */
    ReloadableJettySslFactory(SslContextFactory factory) {
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public void reload() {
        try {
            factory.unload(ReloadableJettySslFactory::reloadLocked);
        }
        catch (ReloadDone done) {
            if (done.err != null)
                throw new IgniteException(done.err);
        }
    }

    /** Reads the date from the keys that new connections use, including keys that the factory loaded without Ignite. */
    @Override public long hotReloadTime() {
        KeyStore store = factory.getKeyStore();

        try {
            return store == null ? 0 : HotReloadTime.of(store);
        }
        catch (GeneralSecurityException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Reloads the factory while {@code unload(...)} holds its lock. If loading fails, the factory loads its previous stores.
     * Always throws {@link ReloadDone}, so that {@code unload(...)} does not unload the factory.
     */
    private static void reloadLocked(SslContextFactory f) {
        KeyStore keyStore = f.getKeyStore();
        KeyStore trustStore = f.getTrustStore();

        try {
            f.reload(ignored -> {});
        }
        catch (Exception e) {
            restore(f, keyStore, trustStore, e);

            throw new ReloadDone(e);
        }

        throw new ReloadDone(null);
    }

    /** Makes the factory load the given stores instead of reading its sources. Its next reload reads its sources again. */
    private static void restore(SslContextFactory f, KeyStore keyStore, KeyStore trustStore, Exception err) {
        f.setKeyStore(keyStore);
        f.setTrustStore(trustStore);

        try {
            f.reload(ignored -> {});
        }
        catch (Exception e) {
            err.addSuppressed(e);
        }
        finally {
            f.setKeyStore(null);
            f.setTrustStore(null);
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
