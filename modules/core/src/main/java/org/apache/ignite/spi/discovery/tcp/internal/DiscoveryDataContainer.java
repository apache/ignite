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
     * Facade interface representing {@link DiscoveryDataContainer} object with discovery data from joining node.
     *
     * It is passed in to {@link org.apache.ignite.internal.GridComponent#onJoiningNodeDataReceived(NewNodeDiscoveryData)} method of each component on all "old" grid nodes.
     */
    public interface NewNodeDiscoveryData {
        /** */
        UUID joiningNodeId();

        /** */
        boolean hasJoiningNodeData();

        /** */
        Serializable joiningNodeData();
    }

    /**
     * Facade interface representing {@link DiscoveryDataContainer} object with discovery data collected in the grid.
     *
     * It is passed in to {@link org.apache.ignite.internal.GridComponent#onGridDataReceived(GridDiscoveryData)} method of each component on new joining node when all discovery data is collected in the grid.
     */
    public interface GridDiscoveryData {
        /** */
        UUID joiningNodeId();

        /** */
        Serializable commonData();

        /** */
        Map<UUID, Serializable> nodeSpecificData();
    }

    /**
     *
     */
    private final class NewNodeDiscoveryDataImpl implements NewNodeDiscoveryData, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int cmpId;

        /** {@inheritDoc} */
        @Override public UUID joiningNodeId() {
            return joiningNodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean hasJoiningNodeData() {
            return unmarshJoiningNodeDiscoData.containsKey(cmpId);
        }

        /** {@inheritDoc} */
        @Override @Nullable public Serializable joiningNodeData() {
            return unmarshJoiningNodeDiscoData.get(cmpId);
        }

        /**
         * @param cmpId Cmp id.
         */
        private void setComponentId(int cmpId) {
            this.cmpId = cmpId;
        }
    }

    /**
     *
     */
    private final class GridDiscoveryDataImpl implements GridDiscoveryData, Serializable {
        /** */
        private static final long serialVersionUID = 0L;
        
        /** */
        private int cmpId;

        /** */
        private Map<UUID, Serializable> nodeSpecificData = new LinkedHashMap<>(unmarshNodeSpecData.size());

        /** {@inheritDoc} */
        @Override public UUID joiningNodeId() {
            return joiningNodeId;
        }

        /** {@inheritDoc} */
        @Override @Nullable public Serializable commonData() {
            return unmarshCmnData.get(cmpId);
        }

        /** {@inheritDoc} */
        @Override public Map<UUID, Serializable> nodeSpecificData() {
            return nodeSpecificData;
        }

        /**
         * @param cmpId Cmp id.
         */
        private void componentId(int cmpId) {
            this.cmpId = cmpId;
            reinitNodeSpecData(cmpId);
        }

        /**
         * @param cmpId Cmp id.
         */
        private void reinitNodeSpecData(int cmpId) {
            nodeSpecificData.clear();
            for (Map.Entry<UUID, Map<Integer, Serializable>> e : unmarshNodeSpecData.entrySet())
                if (e.getValue() != null && e.getValue().containsKey(cmpId))
                    nodeSpecificData.put(e.getKey(), e.getValue().get(cmpId));
        }
    }

    /** */
    private NewNodeDiscoveryDataImpl newJoinerData;

    /** */
    private GridDiscoveryDataImpl gridData;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final UUID DEFAULT_KEY = null;

    /**
     *
     */
    private enum DiscoveryPhase {
        /** */
        JOINING_NODE_DATA_COLLECTION,

        /** */
        GRID_DATA_COLLECTION
    }

    /** */
    private DiscoveryPhase phase;

    /** */
    private UUID joiningNodeId;
    /** */
    private boolean clientNode;

    /** */
    private Map<Integer, byte[]> joiningNodeDiscoData;

    /** */
    private Map<Integer, byte[]> commonDiscoData = new HashMap<>();

    /** */
    private Map<UUID, Map<Integer, byte[]>> nodeSpecificDiscoData = new LinkedHashMap<>();


    /** */
    private Map<Integer, Serializable> unmarshJoiningNodeDiscoData;

    /** */
    private Map<Integer, Serializable> unmarshCmnData;

    /** */
    private Map<UUID, Map<Integer, Serializable>> unmarshNodeSpecData;

    /**
     * @param joiningNodeId Joining node id.
     * @param clientNode Client node.
     */
    public DiscoveryDataContainer(UUID joiningNodeId, boolean clientNode) {
        this.joiningNodeId = joiningNodeId;
        this.clientNode = clientNode;
        phase = DiscoveryPhase.JOINING_NODE_DATA_COLLECTION;
    }

    /**
     * @param cmpId Cmp id.
     * @param data Data.
     */
    public void addGridCommonData(int cmpId, Serializable data) {
        if (isJoiningPhase())
            addDataOnJoin(cmpId, data);
        else {
            if (unmarshCmnData == null)
                unmarshCmnData = new HashMap<>();

            unmarshCmnData.put(cmpId, data);
        }
    }

    /**
     * @param cmpId Cmp id.
     * @param data Data.
     */
    public void addNodeSpecificData(int cmpId, Serializable data) {
        if (unmarshNodeSpecData == null)
            unmarshNodeSpecData = new LinkedHashMap<>();

        if (isJoiningPhase())
            addDataOnJoin(cmpId, data);
        else {
            if (!unmarshNodeSpecData.containsKey(DEFAULT_KEY))
                unmarshNodeSpecData.put(DEFAULT_KEY, new HashMap<Integer, Serializable>());

            unmarshNodeSpecData.get(DEFAULT_KEY).put(cmpId, data);
        }
    }

    /**
     * @param cmpId Cmp id.
     * @param data Data.
     */
    private void addDataOnJoin(int cmpId, Serializable data) {
        if (unmarshNodeSpecData == null)
            unmarshNodeSpecData = new HashMap<>();

        if (!unmarshNodeSpecData.containsKey(DEFAULT_KEY))
            unmarshNodeSpecData.put(DEFAULT_KEY, new HashMap<Integer, Serializable>());

        unmarshNodeSpecData.get(DEFAULT_KEY).put(cmpId, data);
    }

    /**
     * @param cmpId Cmp id.
     */
    public boolean isCommonDataCollectedFor(int cmpId) {
        return commonDiscoData.containsKey(cmpId);
    }

    /**
     *
     */
    public void markGridDiscoveryStarted() {
        phase = DiscoveryPhase.GRID_DATA_COLLECTION;
    }

    /**
     *
     */
    private boolean isJoiningPhase() {
        return phase == DiscoveryPhase.JOINING_NODE_DATA_COLLECTION;
    }

    /**
     * @param nodeId Node id.
     * @param marsh Marsh.
     * @param log Logger.
     */
    public void marshalGridData(UUID nodeId, Marshaller marsh, IgniteLogger log) {
        if (phase == DiscoveryPhase.JOINING_NODE_DATA_COLLECTION) {
            if (unmarshNodeSpecData == null)
                return;

            Map<Integer, Serializable> joiningNodeData = unmarshNodeSpecData.get(DEFAULT_KEY);

            if (joiningNodeData == null || joiningNodeData.isEmpty())
                return;

            joiningNodeDiscoData = U.newHashMap(joiningNodeData.size());
            marshalFromTo(joiningNodeData, joiningNodeDiscoData, marsh, log);
        }
        else {
            marshalFromTo(unmarshCmnData, commonDiscoData, marsh, log);

            if (unmarshNodeSpecData == null)
                return;

            if (unmarshNodeSpecData.containsKey(DEFAULT_KEY)) {
                Map<Integer, byte[]> marshalledNodeSpecData = U.newHashMap(unmarshNodeSpecData.size());
                marshalFromTo(unmarshNodeSpecData.get(DEFAULT_KEY), marshalledNodeSpecData, marsh, log);

                filterDuplicatedData(marshalledNodeSpecData);

                if (!marshalledNodeSpecData.isEmpty())
                    nodeSpecificDiscoData.put(nodeId, marshalledNodeSpecData);
            }
        }

        unmarshCmnData = null;
        unmarshNodeSpecData = null;
    }

    /**
     * TODO https://issues.apache.org/jira/browse/IGNITE-4435
     */
    private void filterDuplicatedData(Map<Integer, byte[]> discoData) {
        for (Map<Integer, byte[]> existingData : nodeSpecificDiscoData.values()) {
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
     * @param src Source.
     * @param target Target.
     * @param marsh Marsh.
     * @param log Logger.
     */
    private void marshalFromTo(Map<Integer, Serializable> src, Map<Integer, byte[]> target, Marshaller marsh, IgniteLogger log) {
        //may happen if nothing was collected from components, corresponding map (for common data or for node specific data) left null
        if (src == null)
            return;

        for (Map.Entry<Integer, Serializable> entry : src.entrySet())
            try {
                target.put(entry.getKey(), marsh.marshal(entry.getValue()));
            } catch (IgniteCheckedException e) {
                U.error(log, "Failed to marshal discovery data " +
                        "[comp=" + entry.getKey() + ", data=" + entry.getValue() + ']', e);
            }
    }

    /**
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param log Logger.
     */
    public void unmarshalGridData(Marshaller marsh, ClassLoader clsLdr, IgniteLogger log) {
        unmarshCmnData = new HashMap<>();
        unmarshNodeSpecData = new LinkedHashMap<>();

        unmarshalFromTo(commonDiscoData, unmarshCmnData, marsh, clsLdr, log);

        for (Map.Entry<UUID, Map<Integer, byte[]>> binDataEntry : nodeSpecificDiscoData.entrySet()) {
            Map<Integer, byte[]> binData = binDataEntry.getValue();
            if (binData == null || binData.isEmpty())
                continue;

            Map<Integer, Serializable> unmarshData = U.newHashMap(binData.size());
            unmarshNodeSpecData.put(binDataEntry.getKey(), unmarshData);

            unmarshalFromTo(binData, unmarshData, marsh, clsLdr, log);
        }
    }

    /**
     *
     */
    public boolean hasJoiningNodeDiscoveryData() {
        return joiningNodeDiscoData != null && !joiningNodeDiscoData.isEmpty();
    }

    /**
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param log Logger.
     */
    public void unmarshalJoiningNodeData(Marshaller marsh, ClassLoader clsLdr, IgniteLogger log) {
        unmarshJoiningNodeDiscoData = new HashMap<>();
        unmarshalFromTo(joiningNodeDiscoData, unmarshJoiningNodeDiscoData, marsh, clsLdr, log);
    }

    /**
     * @param nodeId Node id.
     */
    public boolean hasDataFromNode(UUID nodeId) {
        return nodeSpecificDiscoData.containsKey(nodeId);
    }

    /**
     * @param src Source.
     * @param target Target.
     * @param marsh Marsh.
     * @param clsLdr Class loader.
     * @param log Logger.
     */
    private void unmarshalFromTo(Map<Integer, byte[]> src, Map<Integer, Serializable> target, Marshaller marsh, ClassLoader clsLdr, IgniteLogger log) {
        for (Map.Entry<Integer, byte[]> binEntry : src.entrySet()) {
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

    /**
     *
     */
    public UUID getJoiningNodeId() {
        return joiningNodeId;
    }

    /**
     * @param cmpId Cmp id.
     */
    public GridDiscoveryData gridDiscoveryData(int cmpId) {
        if (gridData == null)
            gridData = new GridDiscoveryDataImpl();
        gridData.componentId(cmpId);
        return gridData;
    }

    /**
     * @param cmpId Cmp id.
     */
    public NewNodeDiscoveryData newJoinerDiscoveryData(int cmpId) {
        if (newJoinerData == null)
            newJoinerData = new NewNodeDiscoveryDataImpl();
        newJoinerData.setComponentId(cmpId);
        return newJoinerData;
    }

    /**
     * Method merges data from current (this) discovery data container with data from provided container.
     *
     * Merging means that if current and provided containers has two data entries of exactly the same data, we force current container to reference data from provided one so duplicated object can be garbage collected.
     * It allows us to save a lot of memory when keeping all message history.
     *
     * @param existingDiscoData existing discovery data we want to merge data entries from.
     * @param mrgdCmnDataKeys common disco data entries already merged for current disco data (no need to merge them again).
     * @param mrgdSpecifDataKeys node specific disco data entries already merged for current disco data.
     */
    public boolean mergeDataFrom(DiscoveryDataContainer existingDiscoData, Collection<Integer> mrgdCmnDataKeys, Collection<UUID> mrgdSpecifDataKeys) {
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
}
