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

package org.apache.ignite.internal.pagemem.size;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.size.region.DataStructureSizeInMemoryRegion;
import org.apache.ignite.internal.pagemem.size.region.DataStructureSizePersistentRegion;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.METRICS;

public class DataStructureSizeNode implements DataStructureSizeContext<DataRegion, DataStructureSizeContext> {
    private static final String NAME = "NODE";

    private final Map<String, DataStructureSizeContext> regions = new ConcurrentLinkedHashMap<>();

    @Override public DataStructureSizeContext parent() {
        return null;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return regions.values();
    }

    @Override public DataStructureSizeContext createChild(DataRegion region) {
        String regionName = region.config().getName();

        int pageSize = region.pageMemory().pageSize();

        DataStructureSizeContext regionSize;

        if (region.config().isPersistenceEnabled())
            regionSize = new DataStructureSizePersistentRegion(this, regionName, pageSize);
        else
            regionSize = new DataStructureSizeInMemoryRegion(this, regionName, pageSize);

        regions.put(regionName, regionSize);

        return regionSize;
    }

    @Override public Collection<DataStructureSize> structures() {
        Collection<DataStructureSize> sizes = new ArrayList<>();

        for (String metricName : METRICS) {
            sizes.add(new DataStructureSizeAdapter() {
                @Override public long size() {
                    long size = 0;

                    for (DataStructureSizeContext region : regions.values())
                        size += region.sizeOf(metricName).size();

                    return size;
                }

                @Override public String name() {
                    return metricName;
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
                return NAME + "-" + name;
            }
        };
    }

    @Override public String name() {
        return NAME;
    }
}
