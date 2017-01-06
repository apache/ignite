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

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

/**
 * Defines partitions set to restrict query results.
 */
public class PartitionSet implements Externalizable {
    /** Range mask. */
    private static final int RANGE_MASK = 1 << 31;

    /** Partition ids. */
    private int[] partIds;

    /**
     * Default constructor.
     */
    public PartitionSet() {
        // No-op.
    }

    /**
     * @param minPart Min partition.
     * @param cnt Count.
     */
    public PartitionSet(int minPart, int cnt) {
        partIds = new int[]{minPart | RANGE_MASK, cnt};
    }

    /**
     * @param partIds Partition ids.
     */
    public PartitionSet(int[] partIds) {
        this.partIds = partIds;
    }

    /**
     * @param partIds Partition ids.
     */
    public PartitionSet(Collection<Integer> partIds) {
        this.partIds = U.toIntArray(partIds);
    }

    /** */
    private boolean isRange() {
        return (partIds[0] & RANGE_MASK) == RANGE_MASK;
    }

    /**
     * Partition iterator.
     */
    public Iterator iterator() {
        if (isRange())
            return new Iterator() {
                int c = 0;

                int start = partIds[0] & ~RANGE_MASK;

                @Override public boolean hasNext() {
                    return c < partIds[1];
                }

                @Override public int next() {
                    return start + c++;
                }
            };
        else
            return new Iterator() {
                int c = 0;

                @Override public boolean hasNext() {
                    return c < partIds.length;
                }

                @Override public int next() {
                    return partIds[c++];
                }
            };
    }

    /**
     * Returns a size of partitions set.
     */
    public int size() {
        return isRange() ? partIds[1] : partIds.length;
    }

    /** Iterator over partitions. */
    public interface Iterator {
        /**
         * @return {@code true} whether there is another partition.
         */
        boolean hasNext();

        /**
         * @return Next partition.
         */
        int next();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(partIds.length);

        for (int partId : partIds)
            out.writeInt(partId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partIds = new int[in.readShort()];

        for (int i = 0; i < partIds.length; i++)
            partIds[i] = in.readInt();
    }
}