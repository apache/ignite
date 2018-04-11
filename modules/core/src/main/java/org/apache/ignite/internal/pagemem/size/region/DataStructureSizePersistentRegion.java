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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.size.group.DataStructureSizePersistentGroup;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.METRICS;

public class DataStructureSizePersistentRegion implements DataStructureSizeContext<CacheGroupContext, DataStructureSizePersistentGroup> {
    private final DataStructureSizeContext parent;

    private final String name;

    private final Map<String, DataStructureSizeContext> groups = new ConcurrentLinkedHashMap<>();

    private final int pageSize;

    public DataStructureSizePersistentRegion(DataStructureSizeContext parent, String name, int pageSize) {
        this.parent = parent;
        this.name = name;
        this.pageSize = pageSize;
    }

    @Override public DataStructureSizeContext parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return groups.values();
    }

    @Override public DataStructureSizePersistentGroup createChild(CacheGroupContext context) {
        String name = context.cacheOrGroupName();

        DataStructureSizePersistentGroup grp = new DataStructureSizePersistentGroup(this, name, pageSize);

        groups.put(name, grp);

        return grp;
    }

    @Override public Collection<DataStructureSize> structures() {
        Collection<DataStructureSize> sizes = new ArrayList<>();

        String regionName = name();

        for (String name : METRICS) {
            sizes.add(new DataStructureSizeAdapter() {
                @Override public long size() {
                    long size = 0;

                    for (DataStructureSizeContext region : groups.values())
                        size += region.sizeOf(name).size();

                    return size;
                }

                @Override public String name() {
                    return regionName + "-" + name;
                }
            });
        }

        return sizes;
    }

    @Override public DataStructureSize sizeOf(String name) {
        return new DataStructureSizeAdapter() {
            @Override public long size() {
                long size = 0;

                for (DataStructureSizeContext region : childes())
                    size += region.sizeOf(name).size();

                return size;
            }

            @Override public String name() {
                return "REGION" + "-" + name() + "-" + name;
            }
        };
    }

    @Override public String name() {
        return name;
    }
}
