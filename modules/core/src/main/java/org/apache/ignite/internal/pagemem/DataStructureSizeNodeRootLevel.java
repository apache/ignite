package org.apache.ignite.internal.pagemem;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.jsr166.ConcurrentLinkedHashMap;

public class DataStructureSizeNodeRootLevel implements DataStructureSizeNode<DataRegion, DataStructureSizeNodeRegionLevel> {
    private final Map<String, DataStructureSizeNode> regions = new ConcurrentLinkedHashMap<>();

    @Override public DataStructureSizeNode parent() {
        return null;
    }

    @Override public Collection<DataStructureSizeNode> childes() {
        return regions.values();
    }

    @Override public DataStructureSizeNodeRegionLevel createChild(DataRegion region) {
        String regionName = region.config().getName();

        DataStructureSizeNodeRegionLevel node = new DataStructureSizeNodeRegionLevel(this, regionName);

        regions.put(regionName, node);

        return node;
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
                return "NODE" + "-" + name;
            }
        };
    }

    @Override public String name() {
        return "NODE";
    }
}
