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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * A wrapper message that holds the mapping of discovery data belonging to a cluster node and is sent to the joining node.
 */
public class NodeSpecificData implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node specific data. */
    @Order(0)
    Map<Integer, byte[]> nodeSpecificData;

    /** Constructor. */
    public NodeSpecificData() {
        // No-op.
    }

    /**
     * @param nodeSpecificData Node specific data.
     */
    public NodeSpecificData(Map<Integer, byte[]> nodeSpecificData) {
        this.nodeSpecificData = nodeSpecificData;
    }

    /**
     * @return Node specific data.
     */
    public Map<Integer, byte[]> nodeSpecificData() {
        return nodeSpecificData;
    }

    /**
     * @param nodeSpecificData New node specific data.
     */
    public void nodeSpecificData(Map<Integer, byte[]> nodeSpecificData) {
        this.nodeSpecificData = nodeSpecificData;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -107;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        NodeSpecificData that = (NodeSpecificData)o;

        return Objects.equals(nodeSpecificData, that.nodeSpecificData);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hashCode(nodeSpecificData);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeSpecificData.class, this);
    }
}
