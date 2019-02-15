/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        return new SSLServerSocketFactoryWrapper(delegate.getServerSocketFactory(), parameters);
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
