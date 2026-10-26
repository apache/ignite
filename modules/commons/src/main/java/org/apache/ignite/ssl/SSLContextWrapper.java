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

package org.apache.ignite.ssl;

import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for {@link SSLContext} with custom SSL parameters and the certificate chain loaded with the context.
 */
public class SSLContextWrapper extends SSLContext {
    /** Chain loaded with the delegate, or {@code null} if unknown. */
    private final X509Certificate[] chain;

    /** Whether the loaded key material is known to contain no private key. */
    private final boolean privateKeyMissing;

    /**
     * Wraps a context with custom SSL parameters. If the delegate is an {@code SSLContextWrapper}, copies its
     * certificate chain and missing-private-key flag.
     *
     * @param delegate Wrapped SSL context.
     * @param sslParameters Extended SSL parameters.
     */
    public SSLContextWrapper(SSLContext delegate, SSLParameters sslParameters) {
        this(delegate, sslParameters, delegate instanceof SSLContextWrapper
            ? ((SSLContextWrapper)delegate).getCertificateChain() : null,
            delegate instanceof SSLContextWrapper && ((SSLContextWrapper)delegate).isPrivateKeyMissing());
    }

    /**
     * Wraps a context with its loaded certificate chain. The chain describes the key material, not the outcome of a TLS
     * handshake. The delegate must not be reinitialized with different key material after wrapping.
     * This constructor uses the supplied chain and sets the missing-private-key flag to {@code false}.
     * It does not copy metadata from the delegate.
     *
     * @param delegate Wrapped SSL context.
     * @param sslParameters Extended SSL parameters, or {@code null} to keep the delegate's parameters.
     * @param chain Chain of the single private key, own certificate first, or {@code null} if unknown.
     */
    public SSLContextWrapper(SSLContext delegate, @Nullable SSLParameters sslParameters, @Nullable X509Certificate[] chain) {
        this(delegate, sslParameters, chain, false);
    }

    /**
     * Wraps a context with its loaded certificate chain and records whether that key material has no private key.
     * Both metadata values come from the arguments, even if the delegate is an {@code SSLContextWrapper}.
     *
     * @param delegate Wrapped SSL context.
     * @param sslParameters Extended SSL parameters, or {@code null} to keep the delegate's parameters.
     * @param chain Chain of the single private key, own certificate first, or {@code null} if unknown.
     * @param privateKeyMissing {@code true} only if the loaded material is known to contain no private key. Do not pass
     *      {@code true} just because the chain is unknown: the material may contain several keys or use custom key managers.
     */
    public SSLContextWrapper(SSLContext delegate, @Nullable SSLParameters sslParameters, @Nullable X509Certificate[] chain,
        boolean privateKeyMissing) {
        super(new DelegatingSSLContextSpi(delegate, sslParameters),
            delegate.getProvider(),
            delegate.getProtocol());

        this.chain = chain == null || chain.length == 0 ? null : chain.clone();
        this.privateKeyMissing = privateKeyMissing;
    }

    /** @return Chain loaded with the context, own certificate first, or {@code null} if unknown. */
    public @Nullable X509Certificate[] getCertificateChain() {
        return chain == null ? null : chain.clone();
    }

    /** @return {@code true} only if the loaded material is known to contain no private key. */
    public boolean isPrivateKeyMissing() {
        return privateKeyMissing;
    }
}
