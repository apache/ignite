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

    /** */
    private UUID nodeId;

    /** */
    private String addresses;

    /** User attribute in result. */
    private Map<String, String> userAttributes;

    /** */
    private List<CacheDistributionGroup> groups;

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
        return addresses;
    }

    /** */
    public void setAddresses(String addresses) {
        this.addresses = addresses;
    }

    /**
     * @return User attribute in result.
     */
    public Map<String, String> getUserAttributes() {
        return userAttributes;
    }

    /**
     * @param userAttributes New user attribute in result.
     */
    public void setUserAttributes(Map<String, String> userAttributes) {
        this.userAttributes = userAttributes;
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
        U.writeString(out, addresses);
        U.writeMap(out, userAttributes);
        U.writeCollection(out, groups);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        addresses = U.readString(in);
        userAttributes = U.readMap(in);
        groups = U.readList(in);
    }

}
