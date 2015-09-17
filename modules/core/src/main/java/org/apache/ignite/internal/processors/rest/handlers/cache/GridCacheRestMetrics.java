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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import java.util.Map;
import org.apache.ignite.internal.util.GridLeanMap;

/**
 * Grid cache metrics for rest.
 */
public class GridCacheRestMetrics {
    /** Number of reads. */
    private int reads;

    /** Number of writes. */
    private int writes;

    /** Number of hits. */
    private int hits;

    /** Number of misses. */
    private int misses;

    /**
     * Constructor.
     *
     * @param reads Reads.
     * @param writes Writes.
     * @param hits Hits.
     * @param misses Misses.
     */
    public GridCacheRestMetrics(int reads, int writes, int hits, int misses) {
        this.reads = reads;
        this.writes = writes;
        this.hits = hits;
        this.misses = misses;
    }

    /**
     * Gets reads.
     *
     * @return Reads.
     */
    public int getReads() {
        return reads;
    }

    /**
     * Sets reads.
     *
     * @param reads Reads.
     */
    public void setReads(int reads) {
        this.reads = reads;
    }

    /**
     * Gets writes.
     *
     * @return Writes.
     */
    public int getWrites() {
        return writes;
    }

    /**
     * Sets writes.
     *
     * @param writes Writes.
     */
    public void setWrites(int writes) {
        this.writes = writes;
    }

    /**
     * Gets hits.
     *
     * @return Hits.
     */
    public int getHits() {
        return hits;
    }

    /**
     * Sets hits.
     *
     * @param hits Hits.
     */
    public void setHits(int hits) {
        this.hits = hits;
    }

    /**
     * Gets misses.
     *
     * @return Misses.
     */
    public int getMisses() {
        return misses;
    }

    /**
     * Sets misses.
     *
     * @param misses Misses.
     */
    public void setMisses(int misses) {
        this.misses = misses;
    }

    /**
     * Creates map with strings.
     *
     * @return Map.
     */
    public Map<String, Long> map() {
        Map<String, Long> map = new GridLeanMap<>(4);

        map.put("reads", (long)reads);
        map.put("writes", (long)writes);
        map.put("hits", (long)hits);
        map.put("misses", (long)misses);

        return map;
    }
}