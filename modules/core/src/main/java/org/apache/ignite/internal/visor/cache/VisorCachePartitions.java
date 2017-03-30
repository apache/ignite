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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for information about cache partitions.
 */
public class VisorCachePartitions implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<VisorCachePartition> primary;

    /** */
    private List<VisorCachePartition> backup;

    /**
     * Default constructor.
     */
    public VisorCachePartitions() {
        primary = new ArrayList<>();
        backup = new ArrayList<>();
    }

    /**
     * Add primary partition descriptor.
     *
     * @param part Partition id.
     * @param heap Number of primary keys in heap.
     * @param offheap Number of primary keys in offheap.
     * @param swap Number of primary keys in swap.
     */
    public void addPrimary(int part, int heap, long offheap, long swap) {
       primary.add(new VisorCachePartition(part, heap, offheap, swap));
    }

    /**
     * Add backup partition descriptor.
     *
     * @param part Partition id.
     * @param heap Number of backup keys in heap.
     * @param offheap Number of backup keys in offheap.
     * @param swap Number of backup keys in swap.
     */
    public void addBackup(int part, int heap, long offheap, long swap) {
       backup.add(new VisorCachePartition(part, heap, offheap, swap));
    }

    /**
     * @return Get list of primary partitions.
     */
    public List<VisorCachePartition> primary() {
        return primary;
    }

    /**
     * @return Get list of backup partitions.
     */
    public List<VisorCachePartition> backup() {
        return backup;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCachePartitions.class, this);
    }
}
