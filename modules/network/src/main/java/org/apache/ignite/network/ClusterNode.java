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
import java.util.UUID;

/**
 * Representation of a node in a cluster.
 */
public class ClusterNode implements Serializable {
    /** Unique name of member in cluster. */
    private final String name;

    /** Node host. */
    private final String host;

    /** Node port. */
    private final int port;

    /**
     * @param name Unique name of member in cluster.
     */
    public ClusterNode(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
    }

    /**
     * @return Unique name of member in cluster.
     */
    public String name() {
        return name;
    }

    /**
     * @return node host name.
     */
    public String host() {
        return host;
    }

    /**
     * @return node port.
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

    /**
     * Creates node UUID.
     *
     * @return Node UUID identifier.
     */
    public UUID id() {
        return new UUID(name.hashCode(), name.substring(name.length() / 2).hashCode());
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
