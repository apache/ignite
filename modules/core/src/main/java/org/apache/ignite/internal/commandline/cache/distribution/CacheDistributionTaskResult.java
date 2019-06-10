/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.cache.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.NotNull;

/**
 * Result of CacheDistributionTask
 */
public class CacheDistributionTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job results. */
    private List<CacheDistributionNode> nodeResList;

    /** Exceptions. */
    private Map<UUID, Exception> exceptions;

    /**
     * @param nodeResList Cluster infos.
     * @param exceptions Exceptions.
     */
    public CacheDistributionTaskResult(List<CacheDistributionNode> nodeResList,
        Map<UUID, Exception> exceptions) {
        this.nodeResList = nodeResList;
        this.exceptions = exceptions;
    }

    /**
     * For externalization only.
     */
    public CacheDistributionTaskResult() {
    }

    /**
     * @return Job results.
     */
    public Collection<CacheDistributionNode> jobResults() {
        return nodeResList;
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, nodeResList);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in
    ) throws IOException, ClassNotFoundException {
        nodeResList = U.readList(in);
        exceptions = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheDistributionTaskResult.class, this);
    }

    /**
     * Print collect information on the distribution of partitions.
     *
     * @param printer Line printer.
     */
    public void print(Consumer<String> printer) {
        if (nodeResList.isEmpty())
            return;

        List<Row> rows = new ArrayList<>();

        for (CacheDistributionNode node : nodeResList) {
            for (CacheDistributionGroup group : node.getGroups()) {
                for (CacheDistributionPartition partition : group.getPartitions()) {
                    final Row row = new Row();
                    row.setGroupId(group.getGroupId());
                    row.setGroupName(group.getGroupName());
                    row.setPartition(partition.getPartition());
                    row.setNodeId(node.getNodeId());
                    row.setPrimary(partition.isPrimary());
                    row.setState(partition.getState());
                    row.setUpdateCounter(partition.getUpdateCounter());
                    row.setSize(partition.getSize());
                    row.setAddresses(node.getAddresses());
                    row.setUserAttributes(node.getUserAttributes());

                    rows.add(row);
                }
            }
        }

        rows.sort(null);

        StringBuilder userAttrsName = new StringBuilder();
        if (!rows.isEmpty() && rows.get(0).userAttrs != null) {
            for (String userAttribute : rows.get(0).userAttrs.keySet()) {
                userAttrsName.append(',');

                if (userAttribute != null)
                    userAttrsName.append(userAttribute);
            }
        }

        printer.accept("[groupId,partition,nodeId,primary,state,updateCounter,partitionSize,nodeAddresses" + userAttrsName + "]");

        int oldGrpId = 0;

        for (Row row : rows) {
            if (oldGrpId != row.grpId) {
                printer.accept("[next group: id=" + row.grpId + ", name=" + row.grpName + ']');

                oldGrpId = row.getGroupId();
            }

            row.print(printer);
        }
    }

    /**
     * Class for
     */
    private static class Row implements Comparable {
        /** */
        private int grpId;

        /** */
        private String grpName;

        /** */
        private int partId;

        /** */
        private UUID nodeId;

        /** */
        private boolean primary;

        /** */
        private GridDhtPartitionState state;

        /** */
        private long updateCntr;

        /** */
        private long size;

        /** */
        private String addrs;

        /** User attribute in result. */
        private Map<String, String> userAttrs;

        /** */
        public int getGroupId() {
            return grpId;
        }

        /** */
        public void setGroupId(int grpId) {
            this.grpId = grpId;
        }

        /** */
        public String getGroupName() {
            return grpName;
        }

        /** */
        public void setGroupName(String grpName) {
            this.grpName = grpName;
        }

        /** */
        public int getPartition() {
            return partId;
        }

        /** */
        public void setPartition(int partId) {
            this.partId = partId;
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
        public boolean isPrimary() {
            return primary;
        }

        /** */
        public void setPrimary(boolean primary) {
            this.primary = primary;
        }

        /** */
        public GridDhtPartitionState getState() {
            return state;
        }

        /** */
        public void setState(GridDhtPartitionState state) {
            this.state = state;
        }

        /** */
        public long getUpdateCounter() {
            return updateCntr;
        }

        /** */
        public void setUpdateCounter(long updateCntr) {
            this.updateCntr = updateCntr;
        }

        /** */
        public long getSize() {
            return size;
        }

        /** */
        public void setSize(long size) {
            this.size = size;
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

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Object o) {
            assert o instanceof Row;

            Row other = (Row)o;

            int res = Integer.compare(grpId, other.grpId);

            if (res == 0) {
                res = Integer.compare(partId, other.partId);

                if (res == 0)
                    res = nodeId.compareTo(other.nodeId);

            }

            return res;
        }

        /** */
        public void print(Consumer<String> printer) {
            StringBuilder sb = new StringBuilder();

            sb.append(grpId);
            sb.append(',');

            sb.append(partId);
            sb.append(',');

            sb.append(U.id8(getNodeId()));
            sb.append(',');

            sb.append(primary ? "P" : "B");
            sb.append(',');

            sb.append(state);
            sb.append(',');

            sb.append(updateCntr);
            sb.append(',');

            sb.append(size);
            sb.append(',');

            sb.append(addrs);

            if (userAttrs != null) {
                for (String userAttribute : userAttrs.values()) {
                    sb.append(',');
                    if (userAttribute != null)
                        sb.append(userAttribute);
                }
            }

            printer.accept(sb.toString());
        }
    }
}
