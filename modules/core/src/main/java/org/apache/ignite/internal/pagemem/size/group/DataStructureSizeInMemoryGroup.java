package org.apache.ignite.internal.pagemem.size.group;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.simpleTracker;

public class DataStructureSizeInMemoryGroup implements DataStructureSizeContext<String, DataStructureSizeContext> {

    private final String name;

    private final DataStructureSizeContext parent;

    private final DataStructureSize pkIndex;

    public DataStructureSizeInMemoryGroup(DataStructureSizeContext parent, String name) {
        this.parent = parent;
        this.name = name;
        this.pkIndex = simpleTracker(name + "-" + PK_INDEX);
    }

    @Override public DataStructureSizeContext parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        throw new UnsupportedOperationException();
    }

    @Override public DataStructureSizeContext createChild(String context) {
        return null;
    }

    @Override public Collection<DataStructureSize> structures() {
        return Collections.emptyList();
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return new DataStructureSizeAdapter() {
            @Override public long size() {
                return 0;
            }

            @Override public String name() {
                return name + "-" + structure;
            }
        };
    }

    @Override public String name() {
        return name;
    }
}
