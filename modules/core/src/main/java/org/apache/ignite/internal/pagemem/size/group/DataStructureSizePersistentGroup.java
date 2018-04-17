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

package org.apache.ignite.internal.pagemem.size.group;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.size.DataStructureSizePartition;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INDEX_REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INDEX_TREE;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PARTITION;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.TOTAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.simpleTracker;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.sizeWithTrackingPages;

public class DataStructureSizePersistentGroup implements DataStructureSizeContext<String, DataStructureSizePartition> {
    private final DataStructureSizeContext parent;

    private final String name;

    private final Map<String, DataStructureSizeContext> parts = new ConcurrentLinkedHashMap<>();

    private final Map<String, DataStructureSize> structures = new ConcurrentLinkedHashMap<>();

    private final int pageSize;

    public DataStructureSizePersistentGroup(DataStructureSizeContext parent, String name, int pageSize) {
        this.pageSize = pageSize;
        this.parent = parent;
        this.name = name;

        DataStructureSize indexesTreePages = simpleTracker(name + "-" + INDEX_TREE);
        DataStructureSize indexesReuseListPages = simpleTracker(name + "-" + INDEX_REUSE_LIST);

        structures.put(indexesTreePages.name(), indexesTreePages);
        structures.put(indexesReuseListPages.name(), indexesReuseListPages);

        DataStructureSize pkIndexPages = simpleTracker(name + "-" + PK_INDEX);
        DataStructureSize reuseListPages = simpleTracker(name + "-" + REUSE_LIST);
        DataStructureSize dataPages = simpleTracker(name + "-" + DATA);
        DataStructureSize pureDataSize = simpleTracker(name + "-" + PURE_DATA);

        structures.put(pkIndexPages.name(), pkIndexPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);

        // Internal size.
        DataStructureSize internalSize = simpleTracker(name + "-" + INTERNAL);

        structures.put(internalSize.name(), internalSize);

        // Index size.
        DataStructureSize indexTotalPages = sizeWithTrackingPages(name + "-" + INDEX, internalSize, pageSize);

        structures.put(indexTotalPages.name(), indexTotalPages);

        // Partitions size.
        DataStructureSize partitionTotalPages = simpleTracker(name + "-" + PARTITION);

        structures.put(partitionTotalPages.name(), partitionTotalPages);

        // Total size.
        DataStructureSize totalPages = new DataStructureSizeAdapter() {
            @Override public long size() {
                return (indexTotalPages.size() + partitionTotalPages.size()) * pageSize + internalSize.size();
            }

            @Override public String name() {
                return name + "-" + TOTAL;
            }
        };

        structures.put(totalPages.name(), totalPages);
    }

    @Override public DataStructureSizeContext parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return parts.values();
    }

    @Override public Collection<DataStructureSize> structures() {
        return structures.values();
    }

    @Override public DataStructureSizePartition createChild(String part) {
        DataStructureSizePartition partLevel = new DataStructureSizePartition(this, part, pageSize);

        parts.put(partLevel.name(), partLevel);

        return partLevel;
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return structures.get(name + "-" + structure);
    }

    @Override public String name() {
        return name;
    }
}
