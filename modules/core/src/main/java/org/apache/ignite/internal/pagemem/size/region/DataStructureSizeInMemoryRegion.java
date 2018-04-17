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

package org.apache.ignite.internal.pagemem.size.region;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.size.group.DataStructureSizeInMemoryGroup;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.TOTAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.simpleTracker;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.sizeAndParentUpdate;

public class DataStructureSizeInMemoryRegion implements DataStructureSizeContext<CacheGroupContext, DataStructureSizeContext> {

    private final DataStructureSizeContext parent;

    private final String name;

    private final int pageSize;

    private final Map<String, DataStructureSizeContext> groups = new ConcurrentLinkedHashMap<>();

    private final Map<String, DataStructureSize> structures = new ConcurrentLinkedHashMap<>();

    public DataStructureSizeInMemoryRegion(DataStructureSizeContext parent, String name, int pageSize) {
        this.parent = parent;
        this.name = name;
        this.pageSize = pageSize;

        DataStructureSize totalSize = new DataStructureSizeAdapter() {
            private final AtomicLong size = new AtomicLong();

            @Override public void inc() {
                add(pageSize);
            }

            @Override public void dec() {

            }

            @Override public void add(long val) {
                size.addAndGet(val);
            }

            @Override public long size() {
                return size.get();
            }

            @Override public String name() {
                return name + "-" + TOTAL;
            }
        };

        DataStructureSize pkIndexPages = sizeAndParentUpdate(name + "-" + PK_INDEX,
            new DataStructureSizeAdapter() {
                @Override public void inc() {
                    totalSize.add(pageSize);
                }
            });

        DataStructureSize reuseListPages = simpleTracker(name + "-" + REUSE_LIST);
        DataStructureSize dataPages = simpleTracker(name + "-" + DATA);
        DataStructureSize pureDataSize = simpleTracker(name + "-" + PURE_DATA);

        structures.put(pkIndexPages.name(), pkIndexPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);
        structures.put(totalSize.name(), totalSize);
    }

    @Override public DataStructureSizeContext parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return groups.values();
    }

    @Override public DataStructureSizeContext createChild(CacheGroupContext context) {
        String cacheOrGroupName = context.cacheOrGroupName();

        DataStructureSizeInMemoryGroup inMemoryGroup = new DataStructureSizeInMemoryGroup(this, cacheOrGroupName);

        groups.put(cacheOrGroupName, inMemoryGroup);

        return inMemoryGroup;
    }

    @Override public Collection<DataStructureSize> structures() {
        return structures.values();
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return structures.get(name + "-" + structure);
    }

    @Override public String name() {
        return name;
    }
}
