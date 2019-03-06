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

package org.apache.ignite.internal.client.impl;

import org.apache.ignite.internal.client.GridClientDataMetrics;

/**
 * Adapter for cache metrics.
 */
public class GridClientDataMetricsAdapter implements GridClientDataMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long createTime = System.currentTimeMillis();

    /** Last read time. */
    private volatile long readTime = System.currentTimeMillis();

    /** Last update time. */
    private volatile long writeTime = System.currentTimeMillis();

    /** Number of reads. */
    private volatile int reads;

    /** Number of writes. */
    private volatile int writes;

    /** Number of hits. */
    private volatile int hits;

    /** Number of misses. */
    private volatile int misses;

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long writeTime() {
        return writeTime;
    }

    /** {@inheritDoc} */
    @Override public long readTime() {
        return readTime;
    }

    /** {@inheritDoc} */
    @Override public int reads() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public int writes() {
        return writes;
    }

    /** {@inheritDoc} */
    @Override public int hits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public int misses() {
        return misses;
    }

    /**
     * Sets creation time.
     *
     * @param createTime Creation time.
     */
    public void createTime(long createTime) {
        this.createTime = createTime;
    }

    /**
     * Sets read time.
     *
     * @param readTime Read time.
     */
    public void readTime(long readTime) {
        this.readTime = readTime;
    }

    /**
     * Sets write time.
     *
     * @param writeTime Write time.
     */
    public void writeTime(long writeTime) {
        this.writeTime = writeTime;
    }

    /**
     * Sets number of reads.
     *
     * @param reads Number of reads.
     */
    public void reads(int reads) {
        this.reads = reads;
    }

    /**
     * Sets number of writes.
     *
     * @param writes Number of writes.
     */
    public void writes(int writes) {
        this.writes = writes;
    }

    /**
     * Sets number of hits.
     *
     * @param hits Number of hits.
     */
    public void hits(int hits) {
        this.hits = hits;
    }

    /**
     * Sets number of misses.
     *
     * @param misses Number of misses.
     */
    public void misses(int misses) {
        this.misses = misses;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientDataMetricsAdapter [" +
            "createTime=" + createTime +
            ", hits=" + hits +
            ", misses=" + misses +
            ", reads=" + reads +
            ", readTime=" + readTime +
            ", writes=" + writes +
            ", writeTime=" + writeTime +
            ']';
    }
}