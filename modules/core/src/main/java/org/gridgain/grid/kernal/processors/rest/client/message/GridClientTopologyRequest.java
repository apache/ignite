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

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.apache.ignite.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * {@code Topology} command request.
 */
public class GridClientTopologyRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Id of requested node. */
    private UUID nodeId;

    /** IP address of requested node. */
    private String nodeIp;

    /** Include metrics flag. */
    private boolean includeMetrics;

    /** Include node attributes flag. */
    private boolean includeAttrs;

    /**
     * @return Include metrics flag.
     */
    public boolean includeMetrics() {
        return includeMetrics;
    }

    /**
     * @param includeMetrics Include metrics flag.
     */
    public void includeMetrics(boolean includeMetrics) {
        this.includeMetrics = includeMetrics;
    }

    /**
     * @return Include node attributes flag.
     */
    public boolean includeAttributes() {
        return includeAttrs;
    }

    /**
     * @param includeAttrs Include node attributes flag.
     */
    public void includeAttributes(boolean includeAttrs) {
        this.includeAttrs = includeAttrs;
    }

    /**
     * @return Node identifier, if specified, {@code null} otherwise.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node identifier to lookup.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Node ip address if specified, {@code null} otherwise.
     */
    public String nodeIp() {
        return nodeIp;
    }

    /**
     * @param nodeIp Node ip address to lookup.
     */
    public void nodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridClientTopologyRequest other = (GridClientTopologyRequest)o;

        return includeAttrs == other.includeAttrs &&
            includeMetrics == other.includeMetrics;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (includeMetrics ? 1 : 0) +
            (includeAttrs ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        super.writePortable(writer);

        PortableRawWriter raw = writer.rawWriter();

        raw.writeUuid(nodeId);
        raw.writeString(nodeIp);
        raw.writeBoolean(includeMetrics);
        raw.writeBoolean(includeAttrs);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        super.readPortable(reader);

        PortableRawReader raw = reader.rawReader();

        nodeId = raw.readUuid();
        nodeIp = raw.readString();
        includeMetrics = raw.readBoolean();
        includeAttrs = raw.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeUuid(out, nodeId);

        U.writeString(out, nodeIp);

        out.writeBoolean(includeMetrics);
        out.writeBoolean(includeAttrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        nodeId = U.readUuid(in);

        nodeIp = U.readString(in);

        includeMetrics = in.readBoolean();
        includeAttrs = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [includeMetrics=" + includeMetrics +
            ", includeAttrs=" + includeAttrs + "]";
    }
}
