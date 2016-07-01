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

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for information about keys in cache partition.
 */
public class VisorCachePartition implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int part;

    /** */
    private int heap;

    /** */
    private long offheap;

    /** */
    private long swap;

    /**
     * Full constructor.
     *
     * @param part Partition id.
     * @param heap Number of keys in heap.
     * @param offheap Number of keys in offheap.
     * @param swap Number of keys in swap.
     */
    public VisorCachePartition(int part, int heap, long offheap, long swap) {
        this.part = part;
        this.heap = heap;
        this.offheap = offheap;
        this.swap = swap;
    }

    /**
     * @return Partition id.
     */
    public int partition() {
        return part;
    }

    /**
     * @return Number of keys in heap.
     */
    public int heap() {
        return heap;
    }

    /**
     * @return Number of keys in offheap.
     */
    public long offheap() {
        return offheap;
    }

    /**
     * @return Number of keys in swap.
     */
    public long swap() {
        return swap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCachePartition.class, this);
    }
}
