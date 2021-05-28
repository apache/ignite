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
package org.apache.ignite.network;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * Representation of a node in a cluster.
 */
public class ClusterNode implements Serializable {
    /** Local id assigned to this node instance. Changes between restarts. */
    private final String id;

    /** Unique name of member in the cluster. Consistent between restarts. */
    private final String name;

    /** Node host. */
    private final String host;

    /** Node port. */
    private final int port;

    /** Node address in host:port format (lazily evaluated) */
    private String address;

    /**
     * @param id The id.
     * @param name The unique node name.
     * @param host The host.
     * @param port The port.
     * @param id Local id that changes between restarts.
     * @param name Unique name of member in cluster.
     * @param host Node host.
     * @param port Node port.
     */
    public ClusterNode(String id, String name, String host, int port) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.port = port;
    }

    /**
     * @return Node's local id.
     */
    public String id() {
        return id;
    }

    /**
     * @return Unique name of member in cluster. Doesn't change between restarts.
     */
    public String name() {
        return name;
    }

    /**
     * @return Node host name.
     */
    public String host() {
        return host;
    }

    /**
     * @return The address.
     */
    public String address() {
        if (address == null)
            address = host + ":" + port;

        return address;
    }

    /**
     * @return Node port.
     */
    public int port() {
        return port;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClusterNode that = (ClusterNode)o;
        return port == that.port && name.equals(that.name) && host.equals(that.host);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, host, port);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterNode.class, this);
    }
}
