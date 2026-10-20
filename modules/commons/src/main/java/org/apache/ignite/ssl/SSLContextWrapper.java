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

    /**
     * @param delegate Wrapped SSL context.
     * @param sslParameters Extended SSL parameters.
     */
    public SSLContextWrapper(SSLContext delegate, SSLParameters sslParameters) {
        this(delegate, sslParameters, delegate instanceof SSLContextWrapper
            ? ((SSLContextWrapper)delegate).getCertificateChain() : null);
    }

    /**
     * Wraps a context with its loaded certificate chain. The chain describes the key material, not the outcome of a TLS
     * handshake. The delegate must not be reinitialized with different key material after wrapping.
     *
     * @param delegate Wrapped SSL context.
     * @param sslParameters Extended SSL parameters, or {@code null} to keep the delegate's parameters.
     * @param chain Chain of the single private key, own certificate first, or {@code null} if unknown.
     */
    public SSLContextWrapper(SSLContext delegate, @Nullable SSLParameters sslParameters, @Nullable X509Certificate[] chain) {
        super(new DelegatingSSLContextSpi(delegate, sslParameters),
            delegate.getProvider(),
            delegate.getProtocol());

        this.chain = chain == null || chain.length == 0 ? null : chain.clone();
    }

    /** @return Chain loaded with the context, own certificate first, or {@code null} if unknown. */
    public @Nullable X509Certificate[] getCertificateChain() {
        return chain == null ? null : chain.clone();
    }
}
