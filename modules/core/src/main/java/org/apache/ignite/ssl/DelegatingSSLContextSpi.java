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

import java.security.KeyManagementException;
import java.security.SecureRandom;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/** */
class DelegatingSSLContextSpi extends SSLContextSpi {

    /** */
    private final SSLContext delegate;

    /** */
    private final SSLParameters parameters;

    /** */
    DelegatingSSLContextSpi(SSLContext delegate, SSLParameters parameters) {
        this.delegate = delegate;
        this.parameters = parameters;
    }

    /** {@inheritDoc} */
    @Override protected void engineInit(KeyManager[] keyManagers, TrustManager[] trustManagers,
        SecureRandom secureRandom) throws KeyManagementException {
        delegate.init(keyManagers, trustManagers, secureRandom);
    }

    /** {@inheritDoc} */
    @Override protected SSLSocketFactory engineGetSocketFactory() {
        return new SSLSocketFactoryWrapper(delegate.getSocketFactory(), parameters);
    }

    /** {@inheritDoc} */
    @Override protected SSLServerSocketFactory engineGetServerSocketFactory() {
        return new SSLServerSocketFactoryWrapper(delegate.getServerSocketFactory(),
            parameters);
    }

    /** {@inheritDoc} */
    @Override protected SSLEngine engineCreateSSLEngine() {
        final SSLEngine engine = delegate.createSSLEngine();

        if (parameters != null)
            engine.setSSLParameters(parameters);

        return engine;
    }

    /** {@inheritDoc} */
    @Override protected SSLEngine engineCreateSSLEngine(String s, int i) {
        final SSLEngine engine = delegate.createSSLEngine();

        if (parameters != null)
            engine.setSSLParameters(parameters);

        return engine;
    }

    /** {@inheritDoc} */
    @Override protected SSLSessionContext engineGetServerSessionContext() {
        return delegate.getServerSessionContext();
    }

    /** {@inheritDoc} */
    @Override protected SSLSessionContext engineGetClientSessionContext() {
        return delegate.getClientSessionContext();
    }

    /** {@inheritDoc} */
    @Override protected SSLParameters engineGetDefaultSSLParameters() {
        return delegate.getDefaultSSLParameters();
    }

    /** {@inheritDoc} */
    @Override protected SSLParameters engineGetSupportedSSLParameters() {
        return delegate.getSupportedSSLParameters();
    }
}
