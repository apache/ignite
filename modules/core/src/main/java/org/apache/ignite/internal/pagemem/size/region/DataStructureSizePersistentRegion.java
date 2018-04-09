package org.apache.ignite.internal.pagemem.size.region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeGroup;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.METRICS;

public class DataStructureSizePersistentRegion implements DataStructureSizeContext<CacheGroupContext, DataStructureSizeGroup> {
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

    @Override public DataStructureSizeGroup createChild(CacheGroupContext context) {
        String name = context.cacheOrGroupName();

        DataStructureSizeGroup grp = new DataStructureSizeGroup(this, name, pageSize);

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
