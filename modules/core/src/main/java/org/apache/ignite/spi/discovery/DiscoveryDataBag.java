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
package org.apache.ignite.spi.discovery;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Provides interface for {@link GridComponent} to collect and exchange discovery data both on
 * joining node and on cluster nodes.
 *
 * It only organizes interaction with components and doesn't provide any capabilities of converting collected data
 * into formats eligible for transmitting over media (like marshalling, compressing and so on).
 */
public class DiscoveryDataBag {
    /**
     * Facade interface representing {@link DiscoveryDataBag} object with discovery data from joining node.
     */
    public interface JoiningNodeDiscoveryData {
        /** @return ID of the joining node. */
        UUID joiningNodeId();

        /** @return Whether joining node provided discovery data. */
        boolean hasJoiningNodeData();

        /** @return Joining node data. */
        Serializable joiningNodeData();
    }

    /**
     * Facade interface representing {@link DiscoveryDataBag} object with discovery data collected in the grid.
     */
    public interface GridDiscoveryData {
        /** @return ID fo the joining node. */
        UUID joiningNodeId();

        /** @return Common for all cluster nodes discovery data that is sent to the joining node. */
        Serializable commonData();

        /** @return Discovery data that is mapped to the particular cluster node and sent to the joining node. */
        Map<UUID, Serializable> nodeSpecificData();
    }

    /**
     *
     */
    private final class JoiningNodeDiscoveryDataImpl implements JoiningNodeDiscoveryData {
        /** */
        private int cmpId;

        /** {@inheritDoc} */
        @Override public UUID joiningNodeId() {
            return joiningNodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean hasJoiningNodeData() {
            return joiningNodeData.containsKey(cmpId);
        }

        /** {@inheritDoc} */
        @Override @Nullable public Serializable joiningNodeData() {
            return joiningNodeData.get(cmpId);
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
    private final class GridDiscoveryDataImpl implements GridDiscoveryData {
        /** */
        private int cmpId;

        /** */
        private Map<UUID, Serializable> nodeSpecificData
                = new LinkedHashMap<>(DiscoveryDataBag.this.nodeSpecificData.size());

        /** {@inheritDoc} */
        @Override public UUID joiningNodeId() {
            return joiningNodeId;
        }

        /** {@inheritDoc} */
        @Override @Nullable public Serializable commonData() {
            if (commonData != null)
                return commonData.get(cmpId);

            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<UUID, Serializable> nodeSpecificData() {
            return nodeSpecificData;
        }

        /**
         * @param cmpId component ID.
         */
        private void componentId(int cmpId) {
            this.cmpId = cmpId;

            reinitNodeSpecData(cmpId);
        }

        /**
         * @param cmpId component ID.
         */
        private void reinitNodeSpecData(int cmpId) {
            nodeSpecificData.clear();

            for (Map.Entry<UUID, Map<Integer, Serializable>> e : DiscoveryDataBag.this.nodeSpecificData.entrySet()) {
                if (e.getValue() != null && e.getValue().containsKey(cmpId))
                    nodeSpecificData.put(e.getKey(), e.getValue().get(cmpId));
            }
        }
    }

    /** Used for collecting node-specific data from component.
     * As component may not know about nodeId it is running on, when component adds node-specific data,
     * it is firstly collected under this key and then moved to another map with a correct UUID key.
     */
    private static final UUID DEFAULT_KEY = null;

    /** */
    private UUID joiningNodeId;

    /**
     * Component IDs with already initialized common discovery data.
     */
    private Set<Integer> cmnDataInitializedCmps;

    /** */
    private Map<Integer, Serializable> joiningNodeData = new HashMap<>();

    /** */
    private Map<Integer, Serializable> commonData = new HashMap<>();

    /** */
    private Map<UUID, Map<Integer, Serializable>> nodeSpecificData = new LinkedHashMap<>();

    /** */
    private JoiningNodeDiscoveryDataImpl newJoinerData;

    /** */
    private GridDiscoveryDataImpl gridData;

    /** */
    private final boolean isJoiningNodeClient;

    /**
     * @param joiningNodeId Joining node id.
     * @param isJoiningNodeClient Flag indicates the joining node is client.
     */
    public DiscoveryDataBag(UUID joiningNodeId, boolean isJoiningNodeClient) {
        this.joiningNodeId = joiningNodeId;
        this.isJoiningNodeClient = isJoiningNodeClient;
    }

    /**
     * @param joiningNodeId Joining node id.
     * @param cmnDataInitializedCmps Component IDs with already initialized common discovery data.
     * @param isJoiningNodeClient Flag indicates the joining node is client.
     */
    public DiscoveryDataBag(UUID joiningNodeId, Set<Integer> cmnDataInitializedCmps, boolean isJoiningNodeClient) {
        this.joiningNodeId = joiningNodeId;
        this.cmnDataInitializedCmps = cmnDataInitializedCmps;
        this.isJoiningNodeClient = isJoiningNodeClient;
    }

    /**
     * @return ID of joining node.
     */
    public UUID joiningNodeId() {
        return joiningNodeId;
    }

    /**
     * @return {@code true} if the joining node is client node. Return {@code false} otherwise.
     */
    public boolean isJoiningNodeClient() {
        return isJoiningNodeClient;
    }

    /**
     * @param cmpId Component ID.
     * @return Discovery data for given component.
     */
    public GridDiscoveryData gridDiscoveryData(int cmpId) {
        if (gridData == null)
            gridData = new GridDiscoveryDataImpl();

        gridData.componentId(cmpId);

        return gridData;
    }

    /**
     * @param cmpId Component ID.
     * @return Joining node discovery data.
     */
    public JoiningNodeDiscoveryData newJoinerDiscoveryData(int cmpId) {
        if (newJoinerData == null)
            newJoinerData = new JoiningNodeDiscoveryDataImpl();

        newJoinerData.setComponentId(cmpId);

        return newJoinerData;
    }

    /**
     * @param cmpId Component ID.
     * @param data Data.
     */
    public void addJoiningNodeData(Integer cmpId, Serializable data) {
        joiningNodeData.put(cmpId, data);
    }

    /**
     * @param cmpId Component ID.
     * @param data Data.
     */
    public void addGridCommonData(Integer cmpId, Serializable data) {
        commonData.put(cmpId, data);
    }

    /**
     * @param cmpId Component ID.
     * @param data Data.
     */
    public void addNodeSpecificData(Integer cmpId, Serializable data) {
        if (!nodeSpecificData.containsKey(DEFAULT_KEY))
            nodeSpecificData.put(DEFAULT_KEY, new HashMap<Integer, Serializable>());

        nodeSpecificData.get(DEFAULT_KEY).put(cmpId, data);
    }

    /**
     * @param cmpId Component ID.
     * @return {@code True} if common data collected for given component.
     */
    public boolean commonDataCollectedFor(Integer cmpId) {
        assert cmnDataInitializedCmps != null;

        return cmnDataInitializedCmps.contains(cmpId);
    }

    /**
     * @param joinNodeData Joining node data.
     */
    public void joiningNodeData(Map<Integer, Serializable> joinNodeData) {
        joiningNodeData.putAll(joinNodeData);
    }

    /**
     * @param cmnData Cmn data.
     */
    public void commonData(Map<Integer, Serializable> cmnData) {
        commonData.putAll(cmnData);
    }

    /**
     * @param nodeSpecData Node specific data.
     */
    public void nodeSpecificData(Map<UUID, Map<Integer, Serializable>> nodeSpecData) {
        nodeSpecificData.putAll(nodeSpecData);
    }

    /** @return Discovery data for each Ignite component that is sent to the cluster nodes by joining node. */
    public Map<Integer, Serializable> joiningNodeData() {
        return joiningNodeData;
    }

    /**
     * @return Discovery data for each Ignite component that is aggregated from the cluster nodes and sent to the
     * joining node.
     */
    public Map<Integer, Serializable> commonData() {
        return commonData;
    }

    /** @return Discovery data that belongs to the current cluster node and is sent to the joining node. */
    @Nullable public Map<Integer, Serializable> localNodeSpecificData() {
        return nodeSpecificData.get(DEFAULT_KEY);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoveryDataBag.class, this);
    }
}
