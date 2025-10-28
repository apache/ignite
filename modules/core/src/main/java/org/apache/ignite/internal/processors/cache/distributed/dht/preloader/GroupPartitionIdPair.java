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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Pair of group ID and partition ID. */
public class GroupPartitionIdPair implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 508;

    /** Group ID. */
    @Order(value = 0, method = "groupId")
    private int grpId;

    /** Partition ID. */
    @Order(value = 1, method = "partitionId")
    private int partId;

    /** Default constructor. */
    public GroupPartitionIdPair() {
        // No-op.
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public GroupPartitionIdPair(int grpId, int partId) {
        this.grpId = grpId;
        this.partId = partId;
    }

    /**
     * @return Group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @param grpId Group ID.
     */
    public void groupId(int grpId) {
        this.grpId = grpId;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @param partId Partition ID.
     */
    public void partitionId(int partId) {
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GroupPartitionIdPair that = (GroupPartitionIdPair)o;

        return grpId == that.grpId && partId == that.partId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(grpId, partId);
    }
}
