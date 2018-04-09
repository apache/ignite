package org.apache.ignite.internal.pagemem.size.region;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.simpleTracker;

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

        DataStructureSize pkIndexPages = simpleTracker(name + "-" + PK_INDEX);
        DataStructureSize reuseListPages = simpleTracker(name + "-" + REUSE_LIST);
        DataStructureSize dataPages = simpleTracker(name + "-" + DATA);
        DataStructureSize pureDataSize = simpleTracker(name + "-" + PURE_DATA);

        structures.put(pkIndexPages.name(), pkIndexPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);
    }

    @Override public DataStructureSizeContext parent() {
        return parent;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return Collections.singletonList(this);
    }

    @Override public DataStructureSizeContext createChild(CacheGroupContext context) {
        return this;
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
