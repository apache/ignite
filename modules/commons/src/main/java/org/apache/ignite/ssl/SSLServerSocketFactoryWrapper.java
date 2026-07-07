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
import java.net.ServerSocket;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

/** */
class SSLServerSocketFactoryWrapper extends SSLServerSocketFactory {

    /** */
    private final SSLServerSocketFactory delegate;

    /** */
    private final SSLParameters parameters;

    /** */
    SSLServerSocketFactoryWrapper(SSLServerSocketFactory delegate, SSLParameters parameters) {
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

    /** {@inheritDoc} */
    @Override public ServerSocket createServerSocket(int port) throws IOException {
        SSLServerSocket srvSock = (SSLServerSocket)delegate.createServerSocket(port);

        if (parameters != null)
            srvSock.setSSLParameters(parameters);

        return srvSock;
    }

    /** {@inheritDoc} */
    @Override public ServerSocket createServerSocket(int port, int backlog) throws IOException {
        SSLServerSocket srvSock = (SSLServerSocket)delegate.createServerSocket(port, backlog);

        if (parameters != null)
            srvSock.setSSLParameters(parameters);

        return srvSock;
    }

    /** {@inheritDoc} */
    @Override public ServerSocket createServerSocket(int port, int backlog, InetAddress locAddr) throws IOException {
        SSLServerSocket srvSock = (SSLServerSocket)delegate.createServerSocket(port, backlog, locAddr);

        if (parameters != null)
            srvSock.setSSLParameters(parameters);

        return srvSock;
    }

}
