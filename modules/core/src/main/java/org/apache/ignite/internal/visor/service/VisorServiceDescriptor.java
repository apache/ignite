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

package org.apache.ignite.internal.visor.service;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Data transfer object for {@link ServiceDescriptor} object.
 */
public class VisorServiceDescriptor extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private String name;

    /** Service class. */
    private String srvcCls;

    /** Maximum allowed total number of deployed services in the grid, {@code 0} for unlimited. */
    private int totalCnt;

    /** Maximum allowed number of deployed services on each node. */
    private int maxPerNodeCnt;

    /** Cache name used for key-to-node affinity calculation. */
    private String cacheName;

    /** ID of grid node that initiated the service deployment. */
    private UUID originNodeId;

    /**
     * Service deployment topology snapshot.
     * Number of service instances deployed on a node mapped to node ID.
     */
    private Map<UUID, Integer> topSnapshot;

    /**
     * Default constructor.
     */
    public VisorServiceDescriptor() {
        // No-op.
    }

    /**
     * Create task result with given parameters
     *
     * @param srvc Service descriptor to transfer.
     */
    public VisorServiceDescriptor(ServiceDescriptor srvc) {
        name = srvc.name();

        try {
            srvcCls = VisorTaskUtils.compactClass(srvc.serviceClass());
        }
        catch (Throwable e) {
            srvcCls = e.getClass().getName() + ": " + e.getMessage();
        }

        totalCnt = srvc.totalCount();
        maxPerNodeCnt = srvc.maxPerNodeCount();
        cacheName = srvc.cacheName();
        originNodeId = srvc.originNodeId();
        topSnapshot = srvc.topologySnapshot();
    }

    /**
     * @return Service name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Service class.
     */
    public String getServiceClass() {
        return srvcCls;
    }

    /**
     * @return Maximum allowed total number of deployed services in the grid, 0 for unlimited.
     */
    public int getTotalCnt() {
        return totalCnt;
    }

    /**
     * @return Maximum allowed number of deployed services on each node.
     */
    public int getMaxPerNodeCnt() {
        return maxPerNodeCnt;
    }

    /**
     * @return Cache name used for key-to-node affinity calculation.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return ID of grid node that initiated the service deployment.
     */
    public UUID getOriginNodeId() {
        return originNodeId;
    }

    /**
     * @return Service deployment topology snapshot. Number of service instances deployed on a node mapped to node ID.
     */
    public Map<UUID, Integer> getTopologySnapshot() {
        return topSnapshot;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeString(out, srvcCls);
        out.writeInt(totalCnt);
        out.writeInt(maxPerNodeCnt);
        U.writeString(out, cacheName);
        U.writeUuid(out, originNodeId);
        U.writeMap(out, topSnapshot);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        srvcCls = U.readString(in);
        totalCnt = in.readInt();
        maxPerNodeCnt = in.readInt();
        cacheName = U.readString(in);
        originNodeId = U.readUuid(in);
        topSnapshot = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorServiceDescriptor.class, this);
    }
}
