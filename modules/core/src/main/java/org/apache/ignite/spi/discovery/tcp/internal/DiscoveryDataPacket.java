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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.SerializableDataBagItemWrapper;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.TRUE;
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
    private boolean joiningNodeClient;

    /** Unmarshalling error, if any. */
    private IgniteCheckedException unmarshErr;

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

    /**
     * @param log Ignite logger.
     * @param client Client mode flag.
     *
     * @return Data bag with node data.
     */
    public DiscoveryDataBag bagWithNodeData(IgniteLogger log, Boolean client) throws IgniteCheckedException {
        checkUnmarshallingErrors(log, client);

        DiscoveryDataBag dataBag = new DiscoveryDataBag(joiningNodeId, joiningNodeClient);

        if (!F.isEmpty(commonData))
            dataBag.commonData(commonData);

        if (!F.isEmpty(nodeSpecificData)) {
            nodeSpecificData.values()
                .removeIf(F::isEmpty);

            dataBag.nodeSpecificData(nodeSpecificData);
        }

        return dataBag;
    }

    /**
     * @param log Ignite logger.
     * @param client Client mode flag.
     *
     * @return Data bag with joining node data.
     */
    public DiscoveryDataBag bagWithJoiningNodeData(IgniteLogger log, @Nullable Boolean client) throws IgniteCheckedException {
        checkUnmarshallingErrors(log, client);

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
     * Dumps and throws catched unmarshalling errors.
     *
     * @param log Ignite logger.
     * @param client Client mode flag.
     * @throws IgniteCheckedException If unmarshalling errors occurs.
     */
    public void checkUnmarshallingErrors(IgniteLogger log, @Nullable Boolean client)
        throws IgniteCheckedException {
        if (unmarshErr != null)
            throw unmarshErr;

        Iterator<Map.Entry<Integer, Message>> allDataIter = allMessages();

        IgniteCheckedException err = null;

        while (allDataIter.hasNext()) {
            Map.Entry<Integer, Message> item = allDataIter.next();

            if (item.getValue() instanceof SerializableDataBagItemWrapper wrapper) {
                int cmpId = item.getKey();

                IgniteCheckedException e = wrapper.unmarshallError();

                if (e != null) {
                    if (CONTINUOUS_PROC.ordinal() == cmpId && X.hasCause(e, ClassNotFoundException.class) && TRUE.equals(client)) {
                        U.warn(log, "Failed to unmarshal continuous query remote filter on client node. " +
                            "Can be ignored.");

                        continue;
                    }
                    else if (cmpId < GridComponent.DiscoveryDataExchangeType.VALUES.length) {
                        U.error(log, "Failed to unmarshal discovery data for component: " +
                            GridComponent.DiscoveryDataExchangeType.VALUES[cmpId], e);
                    }
                    else {
                        U.warn(log, "Failed to unmarshal discovery data." +
                            " Component " + cmpId + " is not found.", e);
                    }

                    if (err == null)
                        err = e;
                    else
                        err.addSuppressed(e);
                }
            }
        }

        if (err != null) {
            unmarshErr = err;

            throw err;
        }
    }

    /** @return Iterator through all messages, stored in DataPacket. */
    private Iterator<Map.Entry<Integer, Message>> allMessages() {
        return F.concat(joiningNodeData.entrySet().iterator(),
            commonData.entrySet().iterator(),
            nodeSpecificData.values()
                .stream()
                .flatMap(m -> m.entrySet().stream())
                .iterator());
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
