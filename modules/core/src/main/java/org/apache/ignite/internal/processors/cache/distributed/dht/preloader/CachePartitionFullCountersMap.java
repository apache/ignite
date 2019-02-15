/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class CachePartitionFullCountersMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long[] initialUpdCntrs;

    /** */
    private long[] updCntrs;

    /**
     * @param other Map to copy.
     */
    public CachePartitionFullCountersMap(CachePartitionFullCountersMap other) {
        initialUpdCntrs = Arrays.copyOf(other.initialUpdCntrs, other.initialUpdCntrs.length);
        updCntrs = Arrays.copyOf(other.updCntrs, other.updCntrs.length);
    }

    /**
     * @param partsCnt Total number of partitions.
     */
    public CachePartitionFullCountersMap(int partsCnt) {
        initialUpdCntrs = new long[partsCnt];
        updCntrs = new long[partsCnt];
    }

    /**
     * Gets an initial update counter by the partition ID.
     *
     * @param p Partition ID.
     * @return Initial update counter for the partition with the given ID.
     */
    public long initialUpdateCounter(int p) {
        return initialUpdCntrs[p];
    }

    /**
     * Gets an update counter by the partition ID.
     *
     * @param p Partition ID.
     * @return Update counter for the partition with the given ID.
     */
    public long updateCounter(int p) {
        return updCntrs[p];
    }

    /**
     * Sets an initial update counter by the partition ID.
     *
     * @param p Partition ID.
     * @param initialUpdCntr Initial update counter to set.
     */
    public void initialUpdateCounter(int p, long initialUpdCntr) {
        initialUpdCntrs[p] = initialUpdCntr;
    }

    /**
     * Sets an update counter by the partition ID.
     *
     * @param p Partition ID.
     * @param updCntr Update counter to set.
     */
    public void updateCounter(int p, long updCntr) {
        updCntrs[p] = updCntr;
    }

    /**
     * Clears full counters map.
     */
    public void clear() {
        Arrays.fill(initialUpdCntrs, 0);
        Arrays.fill(updCntrs, 0);
    }

    /**
     * @param map Full counters map.
     * @return Regular java map with counters.
     */
    public static Map<Integer, T2<Long, Long>> toCountersMap(CachePartitionFullCountersMap map) {
        int partsCnt = map.updCntrs.length;

        Map<Integer, T2<Long, Long>> map0 = U.newHashMap(partsCnt);

        for (int p = 0; p < partsCnt; p++)
            map0.put(p, new T2<>(map.initialUpdCntrs[p], map.updCntrs[p]));

        return map0;
    }

    /**
     * @param map Regular java map with counters.
     * @param partsCnt Total cache partitions.
     * @return Full counters map.
     */
    static CachePartitionFullCountersMap fromCountersMap(Map<Integer, T2<Long, Long>> map, int partsCnt) {
        CachePartitionFullCountersMap map0 = new CachePartitionFullCountersMap(partsCnt);

        for (Map.Entry<Integer, T2<Long, Long>> e : map.entrySet()) {
            T2<Long, Long> cntrs = e.getValue();

            map0.initialUpdCntrs[e.getKey()] = cntrs.get1();
            map0.updCntrs[e.getKey()] = cntrs.get2();
        }

        return map0;
    }
}
