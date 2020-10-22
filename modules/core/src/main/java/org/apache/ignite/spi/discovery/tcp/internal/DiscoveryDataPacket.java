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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONTINUOUS_PROC;

/**
 * Carries discovery data in marshalled form
 * and allows convenient way of converting it to and from {@link DiscoveryDataBag} objects.
 */
public class DiscoveryDataPacket implements Serializable {
    /** Local file header signature(read as a little-endian number). */
    private static int ZIP_HEADER_SIGNATURE = 0x04034b50;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID joiningNodeId;

    /** */
    private Map<Integer, byte[]> joiningNodeData = new HashMap<>();

    /** */
    private transient Map<Integer, Serializable> unmarshalledJoiningNodeData;

    /** */
    private Map<Integer, byte[]> commonData = new HashMap<>();

    /** */
    private Map<UUID, Map<Integer, byte[]>> nodeSpecificData = new LinkedHashMap<>();

    /** */
    private transient boolean joiningNodeClient;

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
    public void marshalGridNodeData(DiscoveryDataBag bag, UUID nodeId, Marshaller marsh, boolean isCompressionEnabled,
        int compressionLevel, IgniteLogger log) {
        marshalData(bag.commonData(), commonData, marsh, isCompressionEnabled, compressionLevel, log);

        Map<Integer, Serializable> locNodeSpecificData = bag.localNodeSpecificData();

        if (locNodeSpecificData != null) {
            Map<Integer, byte[]> marshLocNodeSpecificData = U.newHashMap(locNodeSpecificData.size());

            marshalData(locNodeSpecificData, marshLocNodeSpecificData, marsh, isCompressionEnabled, compressionLevel, log);

            filterDuplicatedData(marshLocNodeSpecificData);

            if (!marshLocNodeSpecificData.isEmpty())
                nodeSpecificData.put(nodeId, marshLocNodeSpecificData);
        }
    }

    /**
     * @param bag Bag.
     * @param marsh Marsh.
     * @param log Logger.
     */
    public void marshalJoiningNodeData(DiscoveryDataBag bag, Marshaller marsh, boolean isCompressionEnabled,
        int compressionLevel, IgniteLogger log) {
        marshalData(bag.joiningNodeData(), joiningNodeData, marsh, isCompressionEnabled, compressionLevel, log);
    }

    /**
     * @return {@code true} if joining node data was transferred via network in zipped format.
     */
    public boolean isJoiningDataZipped() {
        for (Map.Entry<Integer, byte[]> entry : joiningNodeData.entrySet()) {
            if (isZipped(entry.getValue()))
                return true;
        }

        return false;
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

        if (commonData != null && !commonData.isEmpty())
            dataBag.commonData(unmarshalData(commonData, marsh, clsLdr, clientNode, log, true));

        if (nodeSpecificData != null && !nodeSpecificData.isEmpty()) {
            Map<UUID, Map<Integer, Serializable>> unmarshNodeSpecData = U.newLinkedHashMap(nodeSpecificData.size());

            for (Map.Entry<UUID, Map<Integer, byte[]>> nodeBinEntry : nodeSpecificData.entrySet()) {
                Map<Integer, byte[]> nodeBinData = nodeBinEntry.getValue();

                if (nodeBinData == null || nodeBinData.isEmpty())
                    continue;

                unmarshNodeSpecData.put(
                    nodeBinEntry.getKey(),
                    unmarshalData(nodeBinData, marsh, clsLdr, clientNode, log, true)
                );
            }

            dataBag.nodeSpecificData(unmarshNodeSpecData);
        }

        return dataBag;
    }

    /**
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param clientNode Client node.
     * @param log Logger.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public DiscoveryDataBag unmarshalJoiningNodeData(
        Marshaller marsh,
        ClassLoader clsLdr,
        boolean clientNode,
        IgniteLogger log
    ) throws IgniteCheckedException {
        return unmarshalJoiningNodeData(marsh, clsLdr, clientNode, log, true);
    }

    /**
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param clientNode Client node.
     * @param log Logger.
     */
    public DiscoveryDataBag unmarshalJoiningNodeDataSilently(
        Marshaller marsh,
        ClassLoader clsLdr,
        boolean clientNode,
        IgniteLogger log
    ) {
        try {
            return unmarshalJoiningNodeData(marsh, clsLdr, clientNode, log, false);
        }
        catch (IgniteCheckedException impossible) {
            assert false : impossible;

            log.error("Failed to unmarshal joining node data", impossible);

            throw new IgniteException(impossible);
        }
    }

    /**
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param clientNode Client node.
     * @param log Logger.
     * @param panic Throw unmarshalling if {@code true}.
     * @throws IgniteCheckedException If {@code panic} is {@code true} and unmarshalling failed.
     */
    private DiscoveryDataBag unmarshalJoiningNodeData(
        Marshaller marsh,
        ClassLoader clsLdr,
        boolean clientNode,
        IgniteLogger log,
        boolean panic
    ) throws IgniteCheckedException {
        DiscoveryDataBag dataBag = new DiscoveryDataBag(joiningNodeId, joiningNodeClient);

        if (joiningNodeData != null && !joiningNodeData.isEmpty()) {
            unmarshalledJoiningNodeData = unmarshalData(joiningNodeData, marsh, clsLdr, clientNode, log, panic);

            dataBag.joiningNodeData(unmarshalledJoiningNodeData);
        }

        return dataBag;
    }

    /**
     *
     */
    public boolean hasJoiningNodeData() {
        return joiningNodeData != null && !joiningNodeData.isEmpty();
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
            for (Map.Entry<Integer, byte[]> e : commonData.entrySet()) {
                if (!mrgdCmnDataKeys.contains(e.getKey())) {
                    byte[] data = existingDataPacket.commonData.get(e.getKey());

                    if (data != null && Arrays.equals(e.getValue(), data)) {
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
            for (Map.Entry<UUID, Map<Integer, byte[]>> e : nodeSpecificData.entrySet()) {
                if (!mrgdSpecifDataKeys.contains(e.getKey())) {
                    Map<Integer, byte[]> data = existingDataPacket.nodeSpecificData.get(e.getKey());

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
    private boolean mapsEqual(Map<Integer, byte[]> m1, Map<Integer, byte[]> m2) {
        if (m1 == m2)
            return true;

        if (m1.size() == m2.size()) {
            for (Map.Entry<Integer, byte[]> e : m1.entrySet()) {
                byte[] data = m2.get(e.getKey());

                if (!Arrays.equals(e.getValue(), data))
                    return false;
            }

            return true;
        }

        return false;
    }

    /**
     * @param src Source.
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param clientNode Client node.
     * @param log Logger.
     * @param panic Throw unmarshalling if {@code true}.
     * @throws IgniteCheckedException If {@code panic} is {@true} and unmarshalling failed.
     */
    private Map<Integer, Serializable> unmarshalData(
        Map<Integer, byte[]> src,
        Marshaller marsh,
        ClassLoader clsLdr,
        boolean clientNode,
        IgniteLogger log,
        boolean panic
    ) throws IgniteCheckedException {
        Map<Integer, Serializable> res = U.newHashMap(src.size());

        for (Map.Entry<Integer, byte[]> binEntry : src.entrySet()) {
            try {
                Serializable compData = isZipped(binEntry.getValue()) ?
                    U.unmarshalZip(marsh, binEntry.getValue(), clsLdr) :
                    U.unmarshal(marsh, binEntry.getValue(), clsLdr);
                res.put(binEntry.getKey(), compData);
            }
            catch (IgniteCheckedException e) {
                if (CONTINUOUS_PROC.ordinal() == binEntry.getKey() &&
                    X.hasCause(e, ClassNotFoundException.class) && clientNode
                ) {
                    U.warn(log, "Failed to unmarshal continuous query remote filter on client node. Can be ignored.");

                    continue;
                }
                else if (binEntry.getKey() < GridComponent.DiscoveryDataExchangeType.VALUES.length) {
                    U.error(log,
                        "Failed to unmarshal discovery data for component: " +
                            GridComponent.DiscoveryDataExchangeType.VALUES[binEntry.getKey()],
                        e
                    );
                }
                else {
                    U.warn(log, "Failed to unmarshal discovery data." +
                        " Component " + binEntry.getKey() + " is not found.");
                }

                if (panic)
                    throw e;
            }
        }

        return res;
    }

    /**
     * @param value Value to check.
     * @return {@code true} if value is zipped.
     */
    private boolean isZipped(byte[] value) {
        return value != null && value.length > 3 && makeInt(value) == ZIP_HEADER_SIGNATURE;
    }

    /**
     * Make int from first 4 bytes in little-endian byte order.
     *
     * @param b Source of bytes.
     * @return Made int.
     */
    private static int makeInt(byte[] b) {
        return (((b[3]) << 24) |
            ((b[2] & 0xff) << 16) |
            ((b[1] & 0xff) << 8) |
            ((b[0] & 0xff)));
    }

    /**
     * @param src Source.
     * @param target Target.
     * @param marsh Marsh.
     * @param log Logger.
     */
    private void marshalData(
            Map<Integer, Serializable> src,
            Map<Integer, byte[]> target,
            Marshaller marsh,
            boolean isCompressionEnabled,
            int compressionLevel,
            IgniteLogger log
    ) {
        //may happen if nothing was collected from components,
        // corresponding map (for common data or for node specific data) left null
        if (src == null)
            return;

        for (Map.Entry<Integer, Serializable> entry : src.entrySet()) {
            try {
                target.put(entry.getKey(), isCompressionEnabled ?
                    U.zip(U.marshal(marsh, entry.getValue()), compressionLevel) :
                    U.marshal(marsh, entry.getValue()));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to marshal discovery data " +
                        "[comp=" + entry.getKey() + ", data=" + entry.getValue() + ']', e);
            }
        }
    }

    /**
     * @param log Logger.
     */
    public void unzipData(IgniteLogger log) {
        for (Map.Entry<Integer, byte[]> entry : joiningNodeData.entrySet()) {
            try {
                entry.setValue(U.unzip(entry.getValue()));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to unzip discovery data " +
                    "[comp=" + entry.getKey() + ']', e);
            }
        }
    }

    /**
     * TODO https://issues.apache.org/jira/browse/IGNITE-4435
     */
    private void filterDuplicatedData(Map<Integer, byte[]> discoData) {
        for (Map<Integer, byte[]> existingData : nodeSpecificData.values()) {
            Iterator<Map.Entry<Integer, byte[]>> it = discoData.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<Integer, byte[]> discoDataEntry = it.next();

                byte[] curData = existingData.get(discoDataEntry.getKey());

                if (Arrays.equals(curData, discoDataEntry.getValue()))
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

        if (unmarshalledJoiningNodeData != null)
            dataBag.joiningNodeData(unmarshalledJoiningNodeData);

        return dataBag;
    }

    /**
     * @param joiningNodeClient Joining node is client flag.
     */
    public void joiningNodeClient(boolean joiningNodeClient) {
        this.joiningNodeClient = joiningNodeClient;
    }

    /**
     * Clears {@link #unmarshalledJoiningNodeData}
     */
    public void clearUnmarshalledJoiningNodeData() {
        unmarshalledJoiningNodeData = null;
    }
}
