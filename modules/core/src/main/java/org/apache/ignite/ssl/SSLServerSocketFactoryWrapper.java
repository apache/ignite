/*
 * File created on Feb 14, 2016
 *
 * Copyright (c) 2016 Carl Harris, Jr
 * and others as noted
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.net.ServerSocket;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

/**
 * A wrapper for an {@link SSLServerSocketFactory} that sets configured SSL
 * parameters on each socket produced by the factory delegate.
 *
 * @author Carl Harris
 */
class SSLServerSocketFactoryWrapper extends SSLServerSocketFactory {

    private final SSLServerSocketFactory delegate;
    private final SSLParameters parameters;

    public SSLServerSocketFactoryWrapper(SSLServerSocketFactory delegate,
        SSLParameters parameters) {
        this.delegate = delegate;
        this.parameters = parameters;
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        SSLServerSocket serverSocket =
            (SSLServerSocket)delegate.createServerSocket(port);
        if (parameters != null)
            serverSocket.setSSLParameters(parameters);
        return serverSocket;
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog)
        throws IOException {
        SSLServerSocket serverSocket =
            (SSLServerSocket)delegate.createServerSocket(port, backlog);
        serverSocket.setSSLParameters(parameters);
        return serverSocket;
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog,
        InetAddress localAddress) throws IOException {
        SSLServerSocket serverSocket =
            (SSLServerSocket)delegate.createServerSocket(port, backlog, localAddress);
        if (parameters != null)
            serverSocket.setSSLParameters(parameters);
        return serverSocket;
    }

}
