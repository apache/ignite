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

package org.apache.ignite.internal.processors.platform.client.cluster;

import java.io.Serializable;

/**
 * Represents a server node endpoint.
 */
public class NodeEndpoint implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String addr;

    /** */
    private final String host;

    /** */
    private final int port;

    /**
     * Constructor.
     *
     * @param addr Address.
     * @param host Host.
     * @param port Port.
     */
    public NodeEndpoint(String addr, String host, int port) {
        this.addr = addr;
        this.host = host;
        this.port = port;
    }

    /**
     * Gets the address.
     *
     * @return Address.
     */
    public String getAddress() {
        return addr;
    }

    /**
     * Gets the host.
     *
     * @return Host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port.
     *
     * @return Port.
     */
    public int getPort() {
        return port;
    }
}
