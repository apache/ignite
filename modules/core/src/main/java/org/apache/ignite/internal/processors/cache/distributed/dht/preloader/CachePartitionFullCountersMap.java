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
import java.util.Map;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class CachePartitionFullCountersMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int partsCnt; 
    
    /** */
    private long[] initialUpdCntrs = null;

    /** */
    private long[] updCntrs = null;

    /**
     * @param other Map to copy.
     */
    public CachePartitionFullCountersMap(CachePartitionFullCountersMap other) {
    	if (other.initialUpdCntrs != null)
    		initialUpdCntrs = Arrays.copyOf(other.initialUpdCntrs, other.initialUpdCntrs.length);
    	
    	if (other.updCntrs != null) 
    		updCntrs = Arrays.copyOf(other.updCntrs, other.updCntrs.length);
    }

    /**
     * @param partsCnt Total number of partitions.
     */
    public CachePartitionFullCountersMap(int partsCnt) {
        this.partsCnt = partsCnt;    
    }

    /**
     * Gets an initial update counter by the partition ID.
     *
     * @param p Partition ID.
     * @return Initial update counter for the partition with the given ID.
     */
    public long initialUpdateCounter(int p) {
    	if (initialUpdCntrs == null) 
    		return 0;
    	else 
    		return initialUpdCntrs[p];
    }

    /**
     * Gets an update counter by the partition ID.
     *
     * @param p Partition ID.
     * @return Update counter for the partition with the given ID.
     */
    public long updateCounter(int p) {
    	if (updCntrs == null) 
    		return 0;
    	else
    		return updCntrs[p];
    }

    /**
     * Sets an initial update counter by the partition ID.
     *
     * @param p Partition ID.
     * @param initialUpdCntr Initial update counter to set.
     */
    public void initialUpdateCounter(int p, long initialUpdCntr) {
    	if (initialUpdCntrs == null) {
    		if (initialUpdCntr != 0L) 
    			initialUpdCntrs = new long[partsCnt];
    		else 
    			return;    		    		
    	}
        initialUpdCntrs[p] = initialUpdCntr;
    }

    /**
     * Sets an update counter by the partition ID.
     *
     * @param p Partition ID.
     * @param updCntr Update counter to set.
     */
    public void updateCounter(int p, long updCntr) {
    	if (updCntrs == null) {
    		if (updCntr != 0L) 
    			updCntrs = new long[partsCnt];
    		else 
    			return;
    	}    
        updCntrs[p] = updCntr;
    }

    /**
     * Clears full counters map.
     */
    public void clear() {
        initialUpdCntrs = null;
        updCntrs = null;
    }

    /**
     * @param map Full counters map.
     * @return Regular java map with counters.
     */
    public static Map<Integer, T2<Long, Long>> toCountersMap(CachePartitionFullCountersMap map) {
    	Map<Integer, T2<Long, Long>> map0 = U.newHashMap(map.partsCnt);
    
    	for (int p = 0; p < map.partsCnt; p++)     		    	
             map0.put(p, new T2<>(map.initialUpdCntrs == null ? 0 : map.initialUpdCntrs[p], map.updCntrs == null ? 0 : map.updCntrs[p]));
    		    	
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

            map0.initialUpdateCounter(e.getKey(), cntrs.get1());
            map0.updateCounter(e.getKey(),cntrs.get2());
        }

        return map0;
    }
}
