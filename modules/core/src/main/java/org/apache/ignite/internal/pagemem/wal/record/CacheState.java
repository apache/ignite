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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class CacheState {
    /** */
    @GridToStringInclude
    private Map<Integer, PartitionState> parts;

    /**
     * @param partId Partition ID to add.
     * @param size Partition size.
     * @param cntr Partition counter.
     */
    public void addPartitionState(int partId, long size, long cntr) {
        if (parts == null)
            parts = new HashMap<>();

        parts.put(partId, new PartitionState(size, cntr));
    }

    /**
     * @return Partitions map.
     */
    public Map<Integer, PartitionState> partitions() {
        return parts == null ? Collections.<Integer, PartitionState>emptyMap() : parts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheState.class, this);
    }

    /**
     *
     */
    public static class PartitionState {
        /** */
        private final long size;

        /** */
        private final long partCnt;

        /**
         * @param size Partition size.
         * @param partCnt Partition counter.
         */
        public PartitionState(long size, long partCnt) {
            this.size = size;
            this.partCnt = partCnt;
        }

        /**
         * @return Partition size.
         */
        public long size() {
            return size;
        }

        /**
         * @return Partition counter.
         */
        public long partitionCounter() {
            return partCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PartitionState.class, this);
        }
    }
}
