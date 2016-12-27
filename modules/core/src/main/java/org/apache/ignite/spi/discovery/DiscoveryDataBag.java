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
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DiscoveryDataBag {
    /**
     * Facade interface representing {@link DiscoveryDataBag} object with discovery data from joining node.
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
     * Facade interface representing {@link DiscoveryDataBag} object with discovery data collected in the grid.
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
    private final class NewNodeDiscoveryDataImpl implements NewNodeDiscoveryData {
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
        private Map<UUID, Serializable> nodeSpecificData = new LinkedHashMap<>(DiscoveryDataBag.this.nodeSpecificData.size());

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
            for (Map.Entry<UUID, Map<Integer, Serializable>> e : DiscoveryDataBag.this.nodeSpecificData.entrySet())
                if (e.getValue() != null && e.getValue().containsKey(cmpId))
                    nodeSpecificData.put(e.getKey(), e.getValue().get(cmpId));
        }
    }

    /** */
    private static final UUID DEFAULT_KEY = null;

    private UUID joiningNodeId;

    private Map<Integer, Serializable> joiningNodeData;

    private Map<Integer, Serializable> commonData;

    private Map<UUID, Map<Integer, Serializable>> nodeSpecificData;

    /** */
    private NewNodeDiscoveryDataImpl newJoinerData;

    /** */
    private GridDiscoveryDataImpl gridData;

    /**
     * @param joiningNodeId Joining node id.
     */
    public DiscoveryDataBag(UUID joiningNodeId) {
        this.joiningNodeId = joiningNodeId;
    }

    /**
     *
     */
    public UUID joiningNodeId() {
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

    public void addJoiningNodeData(Integer cmpId, Serializable data) {
        if (joiningNodeData == null)
            joiningNodeData = new HashMap<>();

        joiningNodeData.put(cmpId, data);
    }

    public void addGridCommonData(Integer cmpId, Serializable data) {
        if (commonData == null)
            commonData = new HashMap<>();

        commonData.put(cmpId, data);
    }

    public void addNodeSpecificData(Integer cmpId, Serializable data) {
        if (nodeSpecificData == null)
            nodeSpecificData = new LinkedHashMap<>();

        if (!nodeSpecificData.containsKey(DEFAULT_KEY))
            nodeSpecificData.put(DEFAULT_KEY, new HashMap<Integer, Serializable>());

        nodeSpecificData.get(DEFAULT_KEY).put(cmpId, data);
    }

    public boolean commonDataCollectedFor(Integer cmpId) {
        if (commonData != null)
            return commonData.containsKey(cmpId);
        else
            return false;
    }

    public void joiningNodeData(Map<Integer, Serializable> joiningNodeData) {
        assert this.joiningNodeData == null : this.joiningNodeData;

        this.joiningNodeData = joiningNodeData;
    }

    public void commonData(Map<Integer, Serializable> commonData) {
        assert this.commonData == null: this.commonData;

        this.commonData = commonData;
    }

    public void nodeSpecificData(Map<UUID, Map<Integer, Serializable>> nodeSpecificData) {
        assert this.nodeSpecificData == null: this.nodeSpecificData;

        this.nodeSpecificData = nodeSpecificData;
    }

    public Map<Integer, Serializable> joiningNodeData() {
        return joiningNodeData;
    }

    public Map<Integer, Serializable> commonData() {
        return commonData;
    }

    @Nullable public Map<Integer, Serializable> currentNodeSpecificData() {
        if (nodeSpecificData != null)
            return nodeSpecificData.get(DEFAULT_KEY);
        return null;
    }

}
