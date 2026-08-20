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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * SSL socket factory that configure additional SSL params for socket.
 */
class SSLSocketFactoryWrapper extends SSLSocketFactory {
    /** */
    private final SSLSocketFactory delegate;

    /** */
    private final SSLParameters parameters;

    /** */
    SSLSocketFactoryWrapper(SSLSocketFactory delegate, SSLParameters parameters) {
        this.delegate = delegate;
        this.parameters = parameters;
    }

    /** {@inheritDoc} */
    @Override public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    /** {@inheritDoc} */
    @Override public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    /**
     * Configure socket with SSL parameters.
     *
     * @param socket Socket to configure.
     * @return Configured socket.
     */
    private Socket configureSocket(Socket socket) {
        if (parameters != null)
            ((SSLSocket)socket).setSSLParameters(parameters);

        return socket;
    }

    /** {@inheritDoc} */
    @Override public Socket createSocket() throws IOException {
        return configureSocket(delegate.createSocket());
    }

    /** {@inheritDoc} */
    @Override public Socket createSocket(Socket sock, String host, int port, boolean autoClose) throws IOException {
        return configureSocket(delegate.createSocket(sock, host, port, autoClose));
    }

    /** {@inheritDoc} */
    @Override public Socket createSocket(String host, int port) throws IOException {
        return configureSocket(delegate.createSocket(host, port));
    }

    /** {@inheritDoc} */
    @Override public Socket createSocket(String host, int port, InetAddress locAddr, int locPort) throws IOException {
        return configureSocket(delegate.createSocket(host, port, locAddr, locPort));
    }

    /** {@inheritDoc} */
    @Override public Socket createSocket(InetAddress addr, int port) throws IOException {
        return configureSocket(delegate.createSocket(addr, port));
    }

    /** {@inheritDoc} */
    @Override public Socket createSocket(InetAddress addr, int port, InetAddress locAddr, int locPort) throws IOException {
        return configureSocket(delegate.createSocket(addr, port, locAddr, locPort));
    }
}
