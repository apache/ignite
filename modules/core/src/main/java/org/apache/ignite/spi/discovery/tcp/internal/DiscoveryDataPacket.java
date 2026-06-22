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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.SerializableDataBagItemWrapper;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONTINUOUS_PROC;

/**
 * Carries discovery data in form of {@link Message}
 * and allows convenient way of converting it to and from {@link DiscoveryDataBag} objects.
 */
public class DiscoveryDataPacket implements Message {
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
     */
    public void addNodeData(DiscoveryDataBag bag, UUID nodeId) {
        if (bag.commonData() != null)
            commonData.putAll(bag.commonData());

        Map<Integer, Message> locNodeSpecificData = bag.localNodeSpecificData();

        if (!F.isEmpty(locNodeSpecificData))
            nodeSpecificData.put(nodeId, locNodeSpecificData);
    }

    /**
     * @param bag Bag.
     */
    public void addJoiningNodeData(DiscoveryDataBag bag) {
        if (!F.isEmpty(bag.joiningNodeData()))
            joiningNodeData.putAll(bag.joiningNodeData());
    }

    /** */
    public DiscoveryDataBag bagWithNodeData() {
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
     * Collects and dumps catched unmarshalling errors.
     *
     * @param ignite Ignite.
     */
    public List<IgniteCheckedException> checkUnmarshallingErrors(Ignite ignite) {
        List<Map.Entry<Integer, Message>> items = Stream.concat(
                Stream.concat(joiningNodeData.entrySet().stream(), commonData.entrySet().stream()),
                nodeSpecificData.values()
                    .stream()
                    .flatMap(m -> m.entrySet().stream()))
            .filter(e -> e.getValue() instanceof SerializableDataBagItemWrapper)
            .collect(Collectors.toList());

        List<IgniteCheckedException> errs = new ArrayList<>(items.size());

        for (Map.Entry<Integer, Message> item : items) {
            int cmpId = item.getKey();
            SerializableDataBagItemWrapper serializableDataBagItemWrapper = (SerializableDataBagItemWrapper)item.getValue();

            IgniteCheckedException e = serializableDataBagItemWrapper.unmarshallError();

            if (e != null) {
                if (CONTINUOUS_PROC.ordinal() == cmpId && X.hasCause(e, ClassNotFoundException.class) &&
                    ignite.configuration().isClientMode()) {
                    U.warn(ignite.log(), "Failed to unmarshal continuous query remote filter on client node. " +
                        "Can be ignored.");

                    continue;
                }
                else if (cmpId < GridComponent.DiscoveryDataExchangeType.VALUES.length) {
                    U.error(ignite.log(), "Failed to unmarshal discovery data for component: " +
                        GridComponent.DiscoveryDataExchangeType.VALUES[cmpId], e);
                }
                else {
                    U.warn(ignite.log(), "Failed to unmarshal discovery data." +
                        " Component " + cmpId + " is not found.", e);
                }

                errs.add(e);
            }
        }

        return errs;
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
