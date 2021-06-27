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

    /** Network address of this node. */
    private final NetworkAddress address;

    /**
     * @param id Local id that changes between restarts.
     * @param name Unique name of a member in a cluster.
     * @param address Node address.
     */
    public ClusterNode(String id, String name, NetworkAddress address) {
        this.id = id;
        this.name = name;
        this.address = address;
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
     * @return Network address of this node.
     */
    public NetworkAddress address() {
        return address;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClusterNode that = (ClusterNode)o;
        return name.equals(that.name) && address.equals(that.address);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, address);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterNode.class, this);
    }
}
