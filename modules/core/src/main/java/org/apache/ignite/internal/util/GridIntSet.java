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

package org.apache.ignite.internal.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Holds set of integers.
 * <p/>
 * Note: implementation is not thread safe.
 */
public class GridIntSet implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ObjectStreamField[] serialPersistentFields = {
        new ObjectStreamField("vals", int[].class),
    };

    /** Range mask. */
    private static final int RANGE_MASK = 1 << 31;

    /** Partition ids. */
    private int[] vals;

    /**
     * @param minPart Min partition.
     * @param cnt Count.
     */
    public GridIntSet(int minPart, int cnt) {
        vals = new int[]{minPart | RANGE_MASK, cnt};
    }

    /**
     * @param vals Partition ids.
     */
    public GridIntSet(int[] vals) {
        this.vals = vals;

        // Make sure partitions are sorted.
        Arrays.sort(this.vals);

        // Validate.
        for (int i = 0; i < vals.length - 1; i++)
            A.ensure(vals[i] != vals[i + 1], "duplicates are not allowed");
    }

    /**
     * @param partIds Partition ids.
     */
    public GridIntSet(Collection<Integer> partIds) {
        this(U.toIntArray(partIds));
    }

    /** */
    private boolean isRange() {
        return (vals[0] & RANGE_MASK) == RANGE_MASK;
    }

    /**
     * Partition iterator.
     *
     *
     */
    public Iterator iterator() {
        if (isRange())
            return new Iterator() {
                int c = 0;

                int start = vals[0] & ~RANGE_MASK;

                @Override public boolean hasNext() {
                    return c < vals[1];
                }

                @Override public int next() {
                    return start + c++;
                }
            };
        else
            return new Iterator() {
                int c = 0;

                @Override public boolean hasNext() {
                    return c < vals.length;
                }

                @Override public int next() {
                    return vals[c++];
                }
            };
    }

    /**
     * Returns a size of partitions set.
     */
    public int size() {
        return isRange() ? vals[1] : vals.length;
    }

    /**
     * Check if set contains a partition.
     *
     * @param partId Partition id.
     */
    public boolean contains(int partId) {
        if (isRange()) {
            int start = vals[0] & ~RANGE_MASK;

            return start <= partId && partId < start + vals[1];
        }
        else
            return Arrays.binarySearch(vals, partId) >= 0;
    }

    /**
     * Iterator over partitions.
     *
     * Partitions are returned in ascending order.
     */
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
}
