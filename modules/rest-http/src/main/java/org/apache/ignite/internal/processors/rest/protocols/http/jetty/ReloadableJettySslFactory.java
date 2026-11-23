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
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ssl.HotReloadTime;
import org.apache.ignite.internal.processors.ssl.ReloadableSslFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * TLS factory of the Jetty HTTPS connector. A failed reload leaves the previous keys and trust settings in use.
 * <p>
 * Jetty clears the TLS state of its factory before it loads the replacement, and new connections take that state under
 * the factory's lock. Jetty lets callers hold this lock only inside {@link SslContextFactory#reload}. So the whole reload
 * runs inside an outer {@code reload()} call, and new connections wait for it to finish: they get either the replacement
 * or the previous context, never a factory without keys.
 */
class ReloadableJettySslFactory implements ReloadableSslFactory {
    /** */
    private final SslContextFactory factory;

    /** */
    private volatile long hotReloadTime;

    /**
     * @param factory Started TLS factory of the HTTPS connector.
     * @throws GeneralSecurityException If the loaded key store cannot be read.
     */
    ReloadableJettySslFactory(SslContextFactory factory) throws GeneralSecurityException {
        this.factory = factory;

        hotReloadTime = hotReloadTime(factory);
    }

    /** {@inheritDoc} */
    @Override public void reload() {
        try {
            factory.reload(this::reloadLocked);
        }
        catch (ReloadDone done) {
            if (done.err != null)
                throw new IgniteException(done.err);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long hotReloadTime() {
        return hotReloadTime;
    }

    /**
     * Reloads the factory while the outer reload holds its lock. If loading fails, gives the factory its previous context
     * back. Always throws {@link ReloadDone}, so that the outer reload does not unload and load the factory again.
     */
    private void reloadLocked(SslContextFactory f) {
        SSLContext prev = activeContext(f);

        try {
            f.reload(ignored -> {});

            hotReloadTime = hotReloadTime(f);
        }
        catch (Exception e) {
            if (prev != null)
                restore(f, prev, e);

            throw new ReloadDone(e);
        }

        throw new ReloadDone(null);
    }

    /**
     * @return Context that new connections get, or {@code null} if a failed reload outside Ignite, such as one by Jetty's
     *     {@code KeyStoreScanner}, has left the factory without one.
     */
    @Nullable private static SSLContext activeContext(SslContextFactory f) {
        try {
            return f.getSslContext();
        }
        catch (IllegalStateException ignored) {
            return null;
        }
    }

    /** Makes the factory install the given context. A preset context makes it skip its key and trust stores. */
    private static void restore(SslContextFactory f, SSLContext ctx, Exception err) {
        f.setSslContext(ctx);

        try {
            f.reload(ignored -> {});
        }
        catch (Exception e) {
            err.addSuppressed(e);
        }
        finally {
            f.setSslContext(null);
        }
    }

    /** @return Hot reload time of the key store that the factory has loaded. */
    private static long hotReloadTime(SslContextFactory f) throws GeneralSecurityException {
        KeyStore store = f.getKeyStore();

        return store == null ? 0 : HotReloadTime.of(store);
    }

    /** Ends the outer reload. */
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
