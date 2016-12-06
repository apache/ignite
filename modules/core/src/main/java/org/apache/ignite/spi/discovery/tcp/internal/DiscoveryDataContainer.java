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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONTINUOUS_PROC;

/**
 * Entity contains all the data collected during discovery process from each {@link org.apache.ignite.internal.GridComponent}.
 *
 * It enables components to collect discovery data in different ways:
 * <ol>
 *     <li>if data for some component needs to be collected only once on any node, it should be added with {@link #addGridCommonData(int, Serializable)}; and other nodes in grid can check if data has been collected using {@link #isCommonDataCollectedFor(int)}
 * </li>
 * <li>
 *     if component on each node has unique data, that this component should collect it using {@link #addNodeSpecificData(int, Serializable)} method. There is no need to pass in current node {@link UUID} as it will be added automatically.
 * </li>
 * </ol>
 */
public final class DiscoveryDataContainer implements Serializable {

    /**
     * Facade interface representing {@link DiscoveryDataContainer} object with discovery data of new joining node.
     *
     * It is passed in to {@link org.apache.ignite.internal.GridComponent#onJoiningNodeDataReceived(NewNodeDiscoveryData)} method of each component on all "old" grid nodes.
     */
    public interface NewNodeDiscoveryData {
        UUID joiningNodeId();

        boolean hasJoiningNodeData();

        Serializable joiningNodeData();
    }

    /**
     * Facade interface representing {@link DiscoveryDataContainer} object with discovery data collected in the grid.
     *
     * It is passed in to {@link org.apache.ignite.internal.GridComponent#onGridDataReceived(GridDiscoveryData)} method of each component on new joining node when all discovery data is collected in the grid.
     */
    public interface GridDiscoveryData {
        UUID joiningNodeId();

        Serializable commonData();

        Map<UUID, Serializable> nodeSpecificData();
    }

    private final class NewNodeDiscoveryDataImpl implements NewNodeDiscoveryData, Serializable {
        private static final long serialVersionUID = 0L;

        private int cmpId;

        @Override public UUID joiningNodeId() {
            return joiningNodeId;
        }

        @Override public boolean hasJoiningNodeData() {
            return unmarshJoiningNodeDiscoData.containsKey(cmpId);
        }

        @Override @Nullable public Serializable joiningNodeData() {
            return unmarshJoiningNodeDiscoData.get(cmpId);
        }

        private void setComponentId(int cmpId) {
            this.cmpId = cmpId;
        }
    }

    private final class GridDiscoveryDataImpl implements GridDiscoveryData, Serializable {
        private static final long serialVersionUID = 0L;
        
        private int cmpId;

        private Map<UUID, Serializable> nodeSpecificData = new LinkedHashMap<>(unmarshNodeSpecData.size());

        @Override public UUID joiningNodeId() {
            return joiningNodeId;
        }

        @Override @Nullable public Serializable commonData() {
            return unmarshCmnData.get(cmpId);
        }

        @Override public Map<UUID, Serializable> nodeSpecificData() {
            return nodeSpecificData;
        }

        private void setComponentId(int cmpId) {
            this.cmpId = cmpId;
            reinitNodeSpecData(cmpId);
        }

        private void reinitNodeSpecData(int cmpId) {
            nodeSpecificData.clear();
            for (Map.Entry<UUID, Map<Integer, Serializable>> e : unmarshNodeSpecData.entrySet())
                if (e.getValue() != null && e.getValue().containsKey(cmpId))
                    nodeSpecificData.put(e.getKey(), e.getValue().get(cmpId));
        }
    }

    private NewNodeDiscoveryDataImpl newJoinerData;

    private GridDiscoveryDataImpl gridData;

    private static final long serialVersionUID = 0L;

    private static final UUID DEFAULT_UUID = null;

    private enum DiscoveryPhase {
        NODE_JOINING,
        GRID_DATA_COLLECTION
    }

    private DiscoveryPhase phase;

    private UUID joiningNodeId;
    private boolean clientNode;

    private Map<Integer, byte[]> joiningNodeDiscoData;
    private Map<Integer, byte[]> commonDiscoData = new HashMap<>();
    private Map<UUID, Map<Integer, byte[]>> nodeSpecificDiscoData = new LinkedHashMap<>();

    private Map<Integer, Serializable> unmarshJoiningNodeDiscoData = new HashMap<>();
    private Map<Integer, Serializable> unmarshCmnData = new HashMap<>();
    private Map<UUID, Map<Integer, Serializable>> unmarshNodeSpecData = new LinkedHashMap<>();

    public DiscoveryDataContainer(UUID joiningNodeId, boolean clientNode) {
        this.joiningNodeId = joiningNodeId;
        this.clientNode = clientNode;
        phase = DiscoveryPhase.NODE_JOINING;
    }

    public void addGridCommonData(int cmpId, Serializable data) {
        if (isJoiningPhase())
            addDataOnJoin(cmpId, data);
        else
            unmarshCmnData.put(cmpId, data);
    }

    public void addNodeSpecificData(int cmpId, Serializable data) {
        if (isJoiningPhase())
            addDataOnJoin(cmpId, data);
        else {
            if (!unmarshNodeSpecData.containsKey(DEFAULT_UUID))
                unmarshNodeSpecData.put(DEFAULT_UUID, new HashMap<Integer, Serializable>());

            unmarshNodeSpecData.get(DEFAULT_UUID).put(cmpId, data);
        }
    }

    private void addDataOnJoin(int cmpId, Serializable data) {
        if (!unmarshNodeSpecData.containsKey(DEFAULT_UUID))
            unmarshNodeSpecData.put(DEFAULT_UUID, new HashMap<Integer, Serializable>());

        unmarshNodeSpecData.get(DEFAULT_UUID).put(cmpId, data);
    }

    public boolean isCommonDataCollectedFor(int cmpId) {
        return commonDiscoData.containsKey(cmpId);
    }

    public void markGridDiscoveryStarted() {
        phase = DiscoveryPhase.GRID_DATA_COLLECTION;
    }

    private boolean isJoiningPhase() {
        return phase == DiscoveryPhase.NODE_JOINING;
    }

    public void marshalGridData(UUID nodeId, Marshaller marsh, IgniteLogger log) {
        if (phase == DiscoveryPhase.NODE_JOINING) {
            Map<Integer, Serializable> joiningNodeData = unmarshNodeSpecData.get(DEFAULT_UUID);

            if (joiningNodeData == null || joiningNodeData.size() == 0)
                return;

            joiningNodeDiscoData = U.newHashMap(joiningNodeData.size());
            marshalFromTo(joiningNodeData, joiningNodeDiscoData, marsh, log);
        } else {
            marshalFromTo(unmarshCmnData, commonDiscoData, marsh, log);

            if (unmarshNodeSpecData.containsKey(DEFAULT_UUID)) {
                Map<Integer, byte[]> marshalledNodeSpecData = U.newHashMap(unmarshNodeSpecData.size());
                nodeSpecificDiscoData.put(nodeId, marshalledNodeSpecData);
                marshalFromTo(unmarshNodeSpecData.get(DEFAULT_UUID), marshalledNodeSpecData, marsh, log);
            }
        }

        unmarshCmnData.clear();
        unmarshNodeSpecData.clear();
    }

    private void marshalFromTo(Map<Integer, Serializable> source, Map<Integer, byte[]> target, Marshaller marsh, IgniteLogger log) {
        for (Map.Entry<Integer, Serializable> entry : source.entrySet())
            try {
                target.put(entry.getKey(), marsh.marshal(entry.getValue()));
            } catch (IgniteCheckedException e) {
                U.error(log, "Failed to marshal discovery data " +
                        "[comp=" + entry.getKey() + ", data=" + entry.getValue() + ']', e);
            }
    }

    public void unmarshalGridData(Marshaller marsh, ClassLoader clsLdr, IgniteLogger log) {
        unmarshalFromTo(commonDiscoData, unmarshCmnData, marsh, clsLdr, log);

        for (Map.Entry<UUID, Map<Integer, byte[]>> binDataEntry : nodeSpecificDiscoData.entrySet()) {
            Map<Integer, byte[]> binData = binDataEntry.getValue();
            if (binData == null || binData.size() == 0)
                continue;

            Map<Integer, Serializable> unmarshData = U.newHashMap(binData.size());
            unmarshNodeSpecData.put(binDataEntry.getKey(), unmarshData);

            unmarshalFromTo(binData, unmarshData, marsh, clsLdr, log);
        }
    }

    public boolean hasJoiningNodeDiscoveryData() {
        return joiningNodeDiscoData != null && joiningNodeDiscoData.size() > 0;
    }

    public void unmarshalJoiningNodeData(Marshaller marsh, ClassLoader clsLdr, IgniteLogger log) {
        unmarshalFromTo(joiningNodeDiscoData, unmarshJoiningNodeDiscoData, marsh, clsLdr, log);
    }

    public boolean hasDataFromNode(UUID nodeId) {
        return nodeSpecificDiscoData.containsKey(nodeId);
    }

    private void unmarshalFromTo(Map<Integer, byte[]> source, Map<Integer, Serializable> target, Marshaller marsh, ClassLoader clsLdr, IgniteLogger log) {
        for (Map.Entry<Integer, byte[]> binEntry : source.entrySet()) {
            try {
                Serializable compData = marsh.unmarshal(binEntry.getValue(), clsLdr);
                target.put(binEntry.getKey(), compData);
            } catch (IgniteCheckedException e) {
                if (CONTINUOUS_PROC.ordinal() == binEntry.getKey() &&
                        X.hasCause(e, ClassNotFoundException.class) && clientNode)
                    U.warn(log, "Failed to unmarshal continuous query remote filter on client node. Can be ignored.");
                else
                    U.error(log, "Failed to unmarshal discovery data for component: "  + binEntry.getKey(), e);
            }
        }
    }

    public UUID getJoiningNodeId() {
        return joiningNodeId;
    }

    public GridDiscoveryData gridDiscoveryData(int cmpId) {
        if (gridData == null)
            gridData = new GridDiscoveryDataImpl();
        gridData.setComponentId(cmpId);
        return gridData;
    }

    public NewNodeDiscoveryData newJoinerDiscoveryData(int cmpId) {
        if (newJoinerData == null)
            newJoinerData = new NewNodeDiscoveryDataImpl();
        newJoinerData.setComponentId(cmpId);
        return newJoinerData;
    }

    public boolean mergeDataFrom(DiscoveryDataContainer existingDiscoData, Set<Integer> mrgdCmnDataKeys, Set<UUID> mrgdSpecifDataKeys) {
        if (commonDiscoData.size() != mrgdCmnDataKeys.size()) {
            for (Map.Entry<Integer, byte[]> e : commonDiscoData.entrySet()) {
                if (!mrgdCmnDataKeys.contains(e.getKey())) {
                    byte[] data = existingDiscoData.commonDiscoData.get(e.getKey());

                    if (data != null && Arrays.equals(e.getValue(), data)) {
                        e.setValue(data);

                        boolean add = mrgdCmnDataKeys.add(e.getKey());

                        assert add;

                        if (mrgdCmnDataKeys.size() == commonDiscoData.size())
                            break;
                    }
                }
            }
        }

        if (nodeSpecificDiscoData.size() != mrgdSpecifDataKeys.size()) {
            for (Map.Entry<UUID, Map<Integer, byte[]>> e : nodeSpecificDiscoData.entrySet()) {
                if (!mrgdSpecifDataKeys.contains(e.getKey())) {
                    Map<Integer, byte[]> data = existingDiscoData.nodeSpecificDiscoData.get(e.getKey());

                    if (data != null && mapsEqual(e.getValue(), data)) {
                        e.setValue(data);

                        boolean add = mrgdSpecifDataKeys.add(e.getKey());

                        assert add;

                        if (mrgdSpecifDataKeys.size() == nodeSpecificDiscoData.size())
                            break;
                    }
                }
            }
        }

        return (mrgdCmnDataKeys.size() == commonDiscoData.size()) && (mrgdSpecifDataKeys.size() == nodeSpecificDiscoData.size());
    }

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

    public void clearGridData() {
        commonDiscoData.clear();
        nodeSpecificDiscoData.clear();
    }
}
