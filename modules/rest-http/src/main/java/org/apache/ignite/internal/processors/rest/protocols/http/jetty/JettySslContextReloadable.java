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
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.ssl.SslCertificates;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Certificate reload of the Jetty connector serving HTTP REST. Jetty rebuilds the context in place and serves no TLS at all after a failed
 * rebuild, so on a failure {@link #reload()} puts the previous context back.
 */
public class JettySslContextReloadable extends SslContextReloadable {
    /** SSL factory of the running connector. */
    private final SslContextFactory.Server sslCtxFactory;

    /** Context the chain below was worked out for: Jetty may also replace the context by itself, when it watches the key store. */
    private SSLContext servedCtx;

    /** Chain {@link #servedCtx} presents, {@code null} if it cannot be told. */
    private X509Certificate[] servedChain;

    /** @param sslCtxFactory SSL factory of the running connector. */
    public JettySslContextReloadable(SslContextFactory.Server sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> transports() {
        return Collections.singleton(HTTP_REST);
    }

    /** {@inheritDoc} */
    @Override public synchronized void reload() throws IgniteCheckedException {
        if (sslCtxFactory.getKeyStorePath() == null)
            throw new IgniteCheckedException("HTTP REST runs on a ready-made SSL context, there is nothing to read again");

        SSLContext cur = jettyContext();

        try {
            sslCtxFactory.reload(f -> f.setSslContext(null));
        }
        catch (Exception e) {
            try {
                sslCtxFactory.reload(f -> f.setSslContext(cur));
            }
            catch (Exception pinFailure) {
                e.addSuppressed(pinFailure);
            }

            throw new IgniteCheckedException("Failed to rebuild the HTTP REST SSL context [keyStore=" + sslCtxFactory.getKeyStorePath() +
                ", trustStore=" + sslCtxFactory.getTrustStorePath() + ']', e);
        }
    }

    /** @return Context Jetty serves, {@code null} if none or if its last rebuild failed. */
    private @Nullable SSLContext jettyContext() {
        try {
            return sslCtxFactory.getSslContext();
        }
        catch (IllegalStateException failedJettyReload) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized @Nullable X509Certificate[] servedChain() {
        SSLContext ctx = jettyContext();

        if (ctx != servedCtx) {
            servedChain = ctx == null ? null : SslCertificates.servedChain(ctx);
            servedCtx = ctx;
        }

        return servedChain;
    }
}
