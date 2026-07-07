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

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Certificate reload for a transport served by a NIO server: the filter creates an {@link javax.net.ssl.SSLEngine}
 * per new session, so replacing its context is enough to serve new connections with the updated certificates.
 */
public class NioSslContextReloadable implements SslContextReloadable {
    /** Factory to rebuild the SSL context with. */
    private final Factory<SSLContext> sslCtxFactory;

    /** Filter to apply the rebuilt context to. */
    private final GridNioSslFilter filter;

    /** Whether the rebuilt context is checked before it is applied. */
    private final boolean interNode;

    /**
     * @param sslCtxFactory Factory to rebuild the SSL context with.
     * @param filter Filter to apply the rebuilt context to.
     * @param interNode Whether this transport connects nodes to each other. Such a transport is checked before the
     *      new context is applied, see {@link SslContextValidator}.
     */
    public NioSslContextReloadable(Factory<SSLContext> sslCtxFactory, GridNioSslFilter filter, boolean interNode) {
        this.sslCtxFactory = sslCtxFactory;
        this.filter = filter;
        this.interNode = interNode;
    }

    /** {@inheritDoc} */
    @Override public boolean reloadSslContext() throws IgniteCheckedException {
        SSLContext sslCtx = rebuild(true);

        if (sslCtx == null)
            return false;

        filter.updateSslContext(sslCtx);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean checkSslContext() throws IgniteCheckedException {
        return rebuild(false) != null;
    }

    /**
     * @param apply Whether the factory should hand the rebuilt context out afterwards.
     * @return Rebuilt context, or {@code null} if the factory returned the one already in use.
     * @throws IgniteCheckedException If the context could not be built or did not pass the check.
     */
    private @Nullable SSLContext rebuild(boolean apply) throws IgniteCheckedException {
        try {
            SSLContext sslCtx = apply
                ? SslContextUtils.reload(sslCtxFactory, filter.sslContext())
                : SslContextUtils.check(sslCtxFactory, filter.sslContext());

            if (sslCtx != null && interNode)
                SslContextValidator.validateInterNode(sslCtx);

            return sslCtx;
        }
        catch (SSLException e) {
            throw new IgniteCheckedException(e);
        }
    }
}
