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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 * DTO for CacheDistributionTask, contains information about node
 */
public class CacheDistributionNode extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node identifier. */
    @Order(value = 0)
    UUID nodeId;

    /** Network addresses. */
    @Order(value = 1)
    String addrs;

    /** User attribute in result. */
    @Order(value = 2)
    Map<String, String> userAttrs;

    /** Information about groups. */
    @Order(value = 3)
    List<CacheDistributionGroup> groups;

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
}
