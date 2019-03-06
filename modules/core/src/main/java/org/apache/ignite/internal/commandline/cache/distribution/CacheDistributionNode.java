/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.commandline.cache.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO for CacheDistributionTask, contains information about node
 */
public class CacheDistributionNode extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node identifier. */
    private UUID nodeId;

    /** Network addresses. */
    private String addrs;

    /** User attribute in result. */
    private Map<String, String> userAttrs;

    /** Information about groups. */
    private List<CacheDistributionGroup> groups;

    /** Default constructor. */
    public CacheDistributionNode() {
    }

    /**
     * @param nodeId Node identifier.
     * @param addrs Network addresses.
     * @param userAttrs Map node user attribute.
     * @param groups Information about groups.
     */
    public CacheDistributionNode(UUID nodeId, String addrs,
        Map<String, String> userAttrs,
        List<CacheDistributionGroup> groups) {
        this.nodeId = nodeId;
        this.addrs = addrs;
        this.userAttrs = userAttrs;
        this.groups = groups;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }

    /** */
    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public String getAddresses() {
        return addrs;
    }

    /** */
    public void setAddresses(String addrs) {
        this.addrs = addrs;
    }

    /**
     * @return User attribute in result.
     */
    public Map<String, String> getUserAttributes() {
        return userAttrs;
    }

    /**
     * @param userAttrs New user attribute in result.
     */
    public void setUserAttributes(Map<String, String> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /** */
    public List<CacheDistributionGroup> getGroups() {
        return groups;
    }

    /** */
    public void setGroups(List<CacheDistributionGroup> groups) {
        this.groups = groups;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        U.writeString(out, addrs);
        U.writeMap(out, userAttrs);
        U.writeCollection(out, groups);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        addrs = U.readString(in);
        userAttrs = U.readMap(in);
        groups = U.readList(in);
    }

}
