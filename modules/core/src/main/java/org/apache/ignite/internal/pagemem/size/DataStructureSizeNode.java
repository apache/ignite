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
