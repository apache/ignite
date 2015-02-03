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

package org.apache.ignite.internal.util.ipc.loopback;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.ipc.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.resources.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Server loopback IPC endpoint.
 */
public class IpcServerTcpEndpoint implements IpcServerEndpoint {
    /** Default endpoint port number. */
    public static final int DFLT_IPC_PORT = 10500;

    /** Server socket. */
    private ServerSocket srvSock;

    /** Port to bind socket to. */
    private int port = DFLT_IPC_PORT;

    /** Host to bind socket to. */
    private String host;

    /** Management endpoint flag. */
    private boolean mgmt;

    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (port <= 0 || port >= 0xffff)
            throw new IpcEndpointBindException("Port value is illegal: " + port);

        try {
            srvSock = new ServerSocket();

            assert host != null;

            srvSock.bind(new InetSocketAddress(U.resolveLocalHost(host), port));

            if (log.isInfoEnabled())
                log.info("IPC server loopback endpoint started [port=" + port + ']');
        }
        catch (IOException e) {
            if (srvSock != null)
                U.closeQuiet(srvSock);

            throw new IpcEndpointBindException("Failed to bind loopback IPC endpoint (is port already in " +
                "use?): " + port, e);
        }
    }

    /** {@inheritDoc} */
    @Override public IpcEndpoint accept() throws IgniteCheckedException {
        try {
            Socket sock = srvSock.accept();

            return new IpcClientTcpEndpoint(sock);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(srvSock);
    }

    /** {@inheritDoc} */
    @Override public int getPort() {
        return port;
    }

    /**
     * Sets port endpoint will be bound to.
     *
     * @param port Port number.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /** {@inheritDoc} */
    @Override public String getHost() {
        return host;
    }

    /**
     * Sets host endpoint will be bound to.
     *
     * @param host Host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /** {@inheritDoc} */
    @Override public boolean isManagement() {
        return mgmt;
    }

    /**
     * Sets management property.
     *
     * @param mgmt flag.
     */
    public void setManagement(boolean mgmt) {
        this.mgmt = mgmt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IpcServerTcpEndpoint.class, this);
    }

    /**
     * Sets configuration properties from the map.
     *
     * @param endpointCfg Map of properties.
     * @throws IgniteCheckedException If invalid property name or value.
     */
    public void setupConfiguration(Map<String, String> endpointCfg) throws IgniteCheckedException {
        for (Map.Entry<String,String> e : endpointCfg.entrySet()) {
            try {
                switch (e.getKey()) {
                    case "type":
                        //Ignore this property
                        break;

                    case "port":
                        setPort(Integer.parseInt(e.getValue()));
                        break;

                    case "host":
                        setHost(e.getValue());
                        break;

                    case "management":
                        setManagement(Boolean.valueOf(e.getValue()));
                        break;

                    default:
                        throw new IgniteCheckedException("Invalid property '" + e.getKey() + "' of " + getClass().getSimpleName());
                }
            }
            catch (Throwable t) {
                if (t instanceof IgniteCheckedException)
                    throw t;

                throw new IgniteCheckedException("Invalid value '" + e.getValue() + "' of the property '" + e.getKey() + "' in " +
                    getClass().getSimpleName(), t);
            }
        }
    }
}
