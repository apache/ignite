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

import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

/** Node access to the SSL context and certificate information managed by a factory. */
public abstract class SslFactoryAccess {
    /**
     * @param factory Configured factory.
     * @return Context cached by the base factory, if initialized.
     */
    protected abstract @Nullable SSLContext context(AbstractSslContextFactory factory);

    /**
     * @param factory Configured factory.
     * @return Validity by key alias, or {@code null} if unavailable for this context.
     */
    protected abstract @Nullable Map<String, SslCertificateValidity> certificates(AbstractSslContextFactory factory);

    /**
     * @param factory Configured factory.
     * @throws SSLException If the replacement cannot be loaded and validated.
     */
    protected abstract void reload(AbstractSslContextFactory factory) throws SSLException;

    /**
     * @param ctx Context to use for new connections.
     * @throws SSLException If it cannot support TLS stream transports.
     */
    public static void checkContext(@Nullable SSLContext ctx) throws SSLException {
        if (ctx == null)
            throw new SSLException("SSL context factory returned null");

        try {
            ctx.createSSLEngine();
            ctx.getSocketFactory();
            ctx.getServerSocketFactory();
        }
        catch (RuntimeException e) {
            throw new SSLException("SSL context cannot create TLS engines and stream sockets: " + e.getMessage(), e);
        }
    }
}
