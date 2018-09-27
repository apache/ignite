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
package org.apache.ignite.internal.commandline.cache.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
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
    private List<CacheDistributionNode> nodeResultList;

    /** Exceptions. */
    private Map<UUID, Exception> exceptions;

    /**
     * @param nodeResultList Cluster infos.
     * @param exceptions Exceptions.
     */
    public CacheDistributionTaskResult(List<CacheDistributionNode> nodeResultList,
        Map<UUID, Exception> exceptions) {
        this.nodeResultList = nodeResultList;
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
        return nodeResultList;
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, nodeResultList);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in
    ) throws IOException, ClassNotFoundException {
        nodeResultList = U.readList(in);
        exceptions = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheDistributionTaskResult.class, this);
    }

    private static class Row implements Comparable {
        /** */
        private int groupId;

        /** */
        private String groupName;

        /** */
        private int partition;

        /** */
        private UUID nodeId;

        /** */
        private boolean primary;

        /** */
        private GridDhtPartitionState state;

        /** */
        private long updateCounter;

        /** */
        private long size;

        /** */
        private String addresses;

        /** User attribute in result. */
        private Map<String, String> userAttributes;

        /** */
        public int getGroupId() {
            return groupId;
        }

        /** */
        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        /** */
        public String getGroupName() {
            return groupName;
        }

        /** */
        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        /** */
        public int getPartition() {
            return partition;
        }

        /** */
        public void setPartition(int partition) {
            this.partition = partition;
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
            return updateCounter;
        }

        /** */
        public void setUpdateCounter(long updateCounter) {
            this.updateCounter = updateCounter;
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
            return addresses;
        }

        /** */
        public void setAddresses(String addresses) {
            this.addresses = addresses;
        }

        /**
         * @return User attribute in result.
         */
        public Map<String, String> getUserAttributes() {
            return userAttributes;
        }

        /**
         * @param userAttrs New user attribute in result.
         */
        public void setUserAttributes(Map<String, String> userAttrs) {
            userAttributes = userAttrs;
        }

        @Override public int compareTo(@NotNull Object o) {
            assert o instanceof Row;

            Row other = (Row)o;

            int result = groupId - other.groupId;

            if (result == 0) {
                result = partition - other.partition;

                if (result == 0)
                    result = nodeId.compareTo(other.nodeId);

            }

            return result;
        }

        /** */
        public void print(PrintStream out) {
            out.print(groupId);
            out.print(',');

            out.print(partition);
            out.print(',');

            out.print(U.id8(getNodeId()));
            out.print(',');

            out.print(primary ? "P" : "B");
            out.print(',');

            out.print(state);
            out.print(',');

            out.print(updateCounter);
            out.print(',');

            out.print(size);
            out.print(',');

            out.print(addresses);

            if (userAttributes!=null){
                for (String userAttribute:userAttributes.values()){
                    out.print(',');
                    if (userAttribute!=null)
                        out.print(userAttribute);
                }
            }

            out.println();
        }
    }

    /**
     * Print collect information on the distribution of partitions.
     *
     * @param out
     */
    public void print(PrintStream out) {
        if (nodeResultList.isEmpty())
            return;

        List<Row> rows = new ArrayList<>();

        for (CacheDistributionNode node : nodeResultList) {
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


        StringBuilder userAttributesName=new StringBuilder();
        if (!rows.isEmpty() && rows.get(0).userAttributes!=null){
            for (String userAttribute:rows.get(0).userAttributes.keySet()){
                userAttributesName.append(',');

                if (userAttribute!=null)
                    userAttributesName.append(userAttribute);
            }
        }
        out.println("[groupId,partition,nodeId,primary,state,updateCounter,partitionSize,nodeAddresses"+userAttributesName+"]");

        int oldGroupId = 0;

        for (Row row : rows) {
            if (oldGroupId != row.groupId) {
                out.println("[next group: id=" + row.groupId + ", name=" + row.groupName + ']');

                oldGroupId = row.getGroupId();
            }

            row.print(out);
        }
    }
}
