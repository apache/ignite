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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class CachePartitionFullCountersMap implements Serializable, Message {
    /** Type code. */
    public static final short TYPE_CODE = 506;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(value = 0, method = "initialUpdateCounters")
    private long[] initUpdCntrs;

    /** */
    @Order(value = 1, method = "updateCounters")
    private long[] updCntrs;

    /**
     * Default constructor.
     */
    public CachePartitionFullCountersMap() {
        // No-op.
    }

    /**
     * @param other Map to copy.
     */
    public CachePartitionFullCountersMap(CachePartitionFullCountersMap other) {
        initUpdCntrs = Arrays.copyOf(other.initUpdCntrs, other.initUpdCntrs.length);
        updCntrs = Arrays.copyOf(other.updCntrs, other.updCntrs.length);
    }

    /**
     * @param partsCnt Total number of partitions.
     */
    public CachePartitionFullCountersMap(int partsCnt) {
        initUpdCntrs = new long[partsCnt];
        updCntrs = new long[partsCnt];
    }

    /**
     * Gets an initial update counter by the partition ID.
     *
     * @param p Partition ID.
     * @return Initial update counter for the partition with the given ID.
     */
    public long initialUpdateCounter(int p) {
        return initUpdCntrs[p];
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
     * @param initUpdCntr Initial update counter to set.
     */
    public void initialUpdateCounter(int p, long initUpdCntr) {
        initUpdCntrs[p] = initUpdCntr;
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

    /** @return Set of partitions which update counter is zero. */
    public Set<Integer> zeroUpdateCounterPartitions() {
        Set<Integer> res = new HashSet<>();

        for (int p = 0; p < updCntrs.length; p++) {
            if (updCntrs[p] == 0)
                res.add(p);
        }

        return res.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(res);
    }

    /**
     * Clears full counters map.
     */
    public void clear() {
        Arrays.fill(initUpdCntrs, 0);
        Arrays.fill(updCntrs, 0);
    }

    /**
     * @return Initial update counters.
     */
    public long[] initialUpdateCounters() {
        return initUpdCntrs;
    }

    /**
     * @param initUpdCntrs Initial update counters.
     */
    public void initialUpdateCounters(long[] initUpdCntrs) {
        this.initUpdCntrs = initUpdCntrs;
    }

    /**
     * @return Update counters.
     */
    public long[] updateCounters() {
        return updCntrs;
    }

    /**
     * @param updCntrs Update counters.
     */
    public void updateCounters(long[] updCntrs) {
        this.updCntrs = updCntrs;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
