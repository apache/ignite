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
package org.apache.ignite.spi.discovery.tcp.internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

/**
 * Carries discovery data in marshalled form
 * and allows convenient way of converting it to and from {@link DiscoveryDataBag} objects.
 */
public class DiscoveryDataPacket implements Serializable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    UUID joiningNodeId;

    /** */
    @Order(1)
    @Compress
    Map<Integer, Message> joiningNodeData = new HashMap<>();

    /** */
    @Order(2)
    @Compress
    Map<Integer, Message> commonData = new HashMap<>();

    /** */
    @Order(3)
    @Compress
    Map<UUID, Map<Integer, Message>> nodeSpecificData = new HashMap<>();

    /** */
    private transient boolean joiningNodeClient;

    /** Constructor. */
    public DiscoveryDataPacket() {
        // No-op.
    }

    /**
     * @param joiningNodeId Joining node id.
     */
    public DiscoveryDataPacket(UUID joiningNodeId) {
        this.joiningNodeId = joiningNodeId;
    }

    /**
     *
     */
    public UUID joiningNodeId() {
        return joiningNodeId;
    }

    /**
     * @param bag Bag.
     * @param nodeId Node id.
     * @param marsh Marsh.
     * @param log Logger.
     */
    public void marshalGridNodeData(DiscoveryDataBag bag, UUID nodeId, Marshaller marsh,
        int compressionLevel, IgniteLogger log) {
        if (bag.commonData() != null)
            commonData.putAll(bag.commonData());

        Map<Integer, Message> locNodeSpecificData = bag.localNodeSpecificData();

        if (locNodeSpecificData != null) {
            filterDuplicatedData(locNodeSpecificData);

            if (!locNodeSpecificData.isEmpty())
                nodeSpecificData.put(nodeId, locNodeSpecificData);
        }
    }

    /**
     * @param bag Bag.
     */
    public void addJoiningNodeData(DiscoveryDataBag bag) {
        if (!F.isEmpty(bag.joiningNodeData()))
            joiningNodeData.putAll(bag.joiningNodeData());
    }

    /**
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param clientNode Client node.
     * @param log Logger.
     */
    public DiscoveryDataBag unmarshalGridData(
        Marshaller marsh,
        ClassLoader clsLdr,
        boolean clientNode,
        IgniteLogger log
    ) throws IgniteCheckedException {
        DiscoveryDataBag dataBag = new DiscoveryDataBag(joiningNodeId, joiningNodeClient);

        if (!F.isEmpty(commonData))
            dataBag.commonData(commonData);

        if (!F.isEmpty(nodeSpecificData))
            dataBag.nodeSpecificData(F.view(nodeSpecificData, uuid -> !F.isEmpty(nodeSpecificData.get(uuid))));

        return dataBag;
    }

    /**
     * @return Data bag with joining node data.
     */
    public DiscoveryDataBag bagWithJoiningNodeData() {
        DiscoveryDataBag dataBag = new DiscoveryDataBag(joiningNodeId, joiningNodeClient);

        if (!F.isEmpty(joiningNodeData))
            dataBag.joiningNodeData(joiningNodeData);

        return dataBag;
    }

    /**
     *
     */
    public boolean hasJoiningNodeData() {
        return !F.isEmpty(joiningNodeData);
    }

    /**
     * @param nodeId Node id.
     */
    public boolean hasDataFromNode(UUID nodeId) {
        return nodeSpecificData.containsKey(nodeId);
    }

    /**
     * @param existingDataPacket Existing data packet.
     * @param mrgdCmnDataKeys Mrgd cmn data keys.
     * @param mrgdSpecifDataKeys Mrgd specif data keys.
     */
    public boolean mergeDataFrom(
            DiscoveryDataPacket existingDataPacket,
            Collection<Integer> mrgdCmnDataKeys,
            Collection<UUID> mrgdSpecifDataKeys
    ) {
        if (commonData.size() != mrgdCmnDataKeys.size()) {
            for (Map.Entry<Integer, Message> e : commonData.entrySet()) {
                if (!mrgdCmnDataKeys.contains(e.getKey())) {
                    Message data = existingDataPacket.commonData.get(e.getKey());

                    if (data != null && Objects.equals(e.getValue(), data)) {
                        e.setValue(data);

                        boolean add = mrgdCmnDataKeys.add(e.getKey());

                        assert add;

                        if (mrgdCmnDataKeys.size() == commonData.size())
                            break;
                    }
                }
            }
        }

        if (nodeSpecificData.size() != mrgdSpecifDataKeys.size()) {
            for (Map.Entry<UUID, Map<Integer, Message>> e : nodeSpecificData.entrySet()) {
                if (!mrgdSpecifDataKeys.contains(e.getKey())) {
                    Map<Integer, Message> data = existingDataPacket.nodeSpecificData.get(e.getKey());

                    if (data != null && mapsEqual(e.getValue(), data)) {
                        e.setValue(data);

                        boolean add = mrgdSpecifDataKeys.add(e.getKey());

                        assert add;

                        if (mrgdSpecifDataKeys.size() == nodeSpecificData.size())
                            break;
                    }
                }
            }
        }

        return (mrgdCmnDataKeys.size() == commonData.size()) && (mrgdSpecifDataKeys.size() == nodeSpecificData.size());
    }

    /**
     * @param m1 first map to compare.
     * @param m2 second map to compare.
     */
    private boolean mapsEqual(Map<Integer, Message> m1, Map<Integer, Message> m2) {
        if (m1 == m2)
            return true;

        if (m1.size() == m2.size()) {
            for (Map.Entry<Integer, Message> e : m1.entrySet()) {
                Message data = m2.get(e.getKey());

                if (!Objects.equals(e.getValue(), data))
                    return false;
            }

            return true;
        }

        return false;
    }

    /** */
    private void filterDuplicatedData(Map<Integer, Message> discoData) {
        for (Map<Integer, Message> existingData : nodeSpecificData.values()) {
            Iterator<Map.Entry<Integer, Message>> it = discoData.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<Integer, Message> discoDataEntry = it.next();

                Message curData = existingData.get(discoDataEntry.getKey());

                if (Objects.equals(curData, discoDataEntry.getValue()))
                    it.remove();
            }

            if (discoData.isEmpty())
                break;
        }
    }

    /**
     * Returns {@link DiscoveryDataBag} aware of components with already initialized common data
     * (e.g. on nodes prior in cluster to the one where this method is called).
     */
    public DiscoveryDataBag bagForDataCollection() {
        DiscoveryDataBag dataBag = new DiscoveryDataBag(joiningNodeId, commonData.keySet(), joiningNodeClient);

        if (joiningNodeData != null)
            dataBag.joiningNodeData(joiningNodeData);

        return dataBag;
    }

    /**
     * @param joiningNodeClient Joining node is client flag.
     */
    public void joiningNodeClient(boolean joiningNodeClient) {
        this.joiningNodeClient = joiningNodeClient;
    }
}
