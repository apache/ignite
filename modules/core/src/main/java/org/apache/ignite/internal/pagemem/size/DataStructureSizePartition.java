package org.apache.ignite.internal.pagemem.size;

import java.util.Collection;
import java.util.Map;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PARTITION;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.doubleSizeUpdate;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.sizeAndParentUpdate;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.sizeWithTrackingPages;

public class DataStructureSizePartition implements DataStructureSizeContext<Void, DataStructureSizeContext> {
    private final DataStructureSizeGroup groupLevel;

    private final String name;

    private final Map<String, DataStructureSize> structures = new ConcurrentLinkedHashMap<>();

    private final int pageSize;

    public DataStructureSizePartition(DataStructureSizeGroup grpLevel, String name, int pageSize) {
        this.groupLevel = grpLevel;
        this.name = name;
        this.pageSize = pageSize;

        DataStructureSize pkIndexPages = sizeAndParentUpdate(name + "-" + PK_INDEX, groupLevel.sizeOf(PK_INDEX));

        DataStructureSize reuseListPages = sizeAndParentUpdate(name + "-" + REUSE_LIST, grpLevel.sizeOf(REUSE_LIST));

        DataStructureSize dataPages = sizeAndParentUpdate(name + "-" + DATA, grpLevel.sizeOf(DATA));

        DataStructureSize pureDataSize = sizeAndParentUpdate(name + "-" + PURE_DATA, grpLevel.sizeOf(PURE_DATA));

        DataStructureSize internalSize = sizeAndParentUpdate(name + "-" + INTERNAL, grpLevel.sizeOf(INTERNAL));

        DataStructureSize partitionSize = sizeWithTrackingPages(name + "-" + PARTITION, internalSize, pageSize);

        DataStructureSize partitionPages = doubleSizeUpdate(partitionSize, grpLevel.sizeOf(PARTITION));

        structures.put(pkIndexPages.name(), pkIndexPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);
        structures.put(internalSize.name(), internalSize);
        structures.put(partitionPages.name(), partitionPages);

    }

    @Override public DataStructureSizeContext parent() {
        return groupLevel;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        throw new UnsupportedOperationException();
    }

    @Override public DataStructureSizeContext createChild(Void context) {
        throw new UnsupportedOperationException();
    }

    @Override public Collection<DataStructureSize> structures() {
        return structures.values();
    }

    @Override public DataStructureSize sizeOf(String struct) {
        return structures.get(name + "-" + struct);
    }

    @Override public String name() {
        return name;
    }
}
