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

package org.apache.ignite.internal.visor.misc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *  Data transfer object for {@link ClusterNode}.
 */
public class VisorClusterNode extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cluster node consistent id. */
    @GridToStringInclude
    private String consistentId;

    /** Cluster node attributes. */
    private Map<String, Object> attrs;

    /** Cluster node addresses. */
    @GridToStringInclude
    private Collection<String> addrs;

    /** Cluster node host names. */
    @GridToStringInclude
    private Collection<String> hostNames;

    /**
     * Default constructor.
     */
    public VisorClusterNode() {
        // No-op.
    }

    /**
     * Create data transfer object for baseline node.
     *
     * @param node Baseline node.
     */
    public VisorClusterNode(ClusterNode node) {
        consistentId = String.valueOf(node.consistentId());
        addrs = node.addresses();
        hostNames = node.hostNames();
        attrs = node.attributes();
    }

    /**
     * Get cluster node consistent id.
     *
     * @return Cluster node consistent id.
     */
    public String getConsistentId() {
        return consistentId;
    }

    /**
     * Get cluster node attributes.
     *
     * @return Cluster node attributes.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }

    /**
     * Get cluster node addresses.
     *
     * @return Node addresses.
     */
    public Collection<String> getAddresses() {
        return addrs;
    }

    /**
     * Get cluster node host names.
     *
     * @return Node host names.
     */
    public Collection<String> getHostNames() {
        return hostNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, consistentId);
        U.writeMap(out, attrs);
        U.writeCollection(out, hostNames);
        U.writeCollection(out, addrs);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        consistentId = U.readString(in);
        attrs = U.readMap(in);
        hostNames = U.readCollection(in);
        addrs = U.readCollection(in);
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorClusterNode.class, this);
    }
}
