package org.apache.ignite.internal.pagemem;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.jsr166.ConcurrentLinkedHashMap;

public class DataStructureSizeNodeRegionLevel implements DataStructureSizeNode<CacheGroupContext, DataStructureSizeNodeGroupLevel> {
    private final DataStructureSizeNode parent;

    private final String name;

    private final Map<String, DataStructureSizeNode> groups = new ConcurrentLinkedHashMap<>();

    public DataStructureSizeNodeRegionLevel(DataStructureSizeNode parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    @Override public DataStructureSizeNode parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeNode> childes() {
        return groups.values();
    }

    @Override public DataStructureSizeNodeGroupLevel createChild(CacheGroupContext context) {
        String name = context.cacheOrGroupName();

        DataStructureSizeNodeGroupLevel grp = new DataStructureSizeNodeGroupLevel(this, name);

        groups.put(name, grp);

        return grp;
    }

    @Override public Collection<DataStructureSize> structures() {
        return Collections.emptyList();
    }

    @Override public DataStructureSize sizeOf(String name) {
        return new DataStructureSizeAdapter() {
            @Override public long size() {
                long size = 0;

                for (DataStructureSizeNode region : childes())
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
