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
import java.net.Socket;
import java.net.UnknownHostException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * A wrapper for an {@link SSLSocketFactory} that sets configured SSL
 * parameters on each socket produced by the factory delegate.
 *
 * @author Carl Harris
 */
class SSLSocketFactoryWrapper extends SSLSocketFactory {

    private final SSLSocketFactory delegate;
    private final SSLParameters parameters;

    public SSLSocketFactoryWrapper(SSLSocketFactory delegate,
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
    public Socket createSocket() throws IOException {
        SSLSocket sslSocket = (SSLSocket)delegate.createSocket();
        if (parameters != null)
            sslSocket.setSSLParameters(parameters);
        return sslSocket;
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port,
        boolean autoClose) throws IOException {
        SSLSocket sslSocket = (SSLSocket)delegate.createSocket(socket, host, port,
            autoClose);
        if (parameters != null)
            sslSocket.setSSLParameters(parameters);
        return sslSocket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException,
        UnknownHostException {
        SSLSocket socket = (SSLSocket)delegate.createSocket(host, port);
        if (parameters != null)
            socket.setSSLParameters(parameters);
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress,
        int localPort) throws IOException, UnknownHostException {
        SSLSocket socket = (SSLSocket)delegate.createSocket(host, port,
            localAddress, localPort);
        if (parameters != null)
            socket.setSSLParameters(parameters);
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress address, int port) throws IOException {
        SSLSocket socket = (SSLSocket)delegate.createSocket(address, port);
        if (parameters != null)
            socket.setSSLParameters(parameters);
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress address, int port,
        InetAddress localAddress, int localPort) throws IOException {
        SSLSocket socket = (SSLSocket)delegate.createSocket(address, port,
            localAddress, localPort);
        if (parameters != null)
            socket.setSSLParameters(parameters);
        return socket;
    }

}
