package org.apache.ignite.internal.pagemem;

import java.util.Collection;
import java.util.Map;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PARTITION;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.doubleSizeUpdate;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.sizeAndParentUpdate;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.sizeWithTrackingPages;

public class DataStructureSizeNodePartitionLevel implements DataStructureSizeNode<Void, DataStructureSizeNode> {
    private final DataStructureSizeNodeGroupLevel groupLevel;

    private final String name;

    private final Map<String, DataStructureSize> structures = new ConcurrentLinkedHashMap<>();

    private final int pageSize = 0;

    public DataStructureSizeNodePartitionLevel(DataStructureSizeNodeGroupLevel grpLevel, String name) {
        this.groupLevel = grpLevel;
        this.name = name;

        DataStructureSize pkIndexPages = sizeAndParentUpdate(name + "-" + PK_INDEX, groupLevel.sizeOf(PK_INDEX));

        DataStructureSize reuseListPages = sizeAndParentUpdate(name + "-" + REUSE_LIST, grpLevel.sizeOf(REUSE_LIST));

        DataStructureSize dataPages = sizeAndParentUpdate(name + "-" + DATA, grpLevel.sizeOf(DATA));

        DataStructureSize pureDataSize = sizeAndParentUpdate(name + "-" + PURE_DATA, grpLevel.sizeOf(PURE_DATA));

        DataStructureSize internalSize = sizeAndParentUpdate(name + "-" + INTERNAL, grpLevel.sizeOf(INTERNAL));

        DataStructureSize partitionSize = sizeWithTrackingPages(name + "-" + PARTITION, internalSize, pageSize);

        DataStructureSize partitionPages = doubleSizeUpdate(partitionSize, grpLevel.sizeOf(PARTITION));

        structures.put(pkIndexPages.name(), partitionPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);
        structures.put(internalSize.name(), internalSize);
        structures.put(partitionPages.name(), partitionPages);

    }

    @Override public DataStructureSizeNode parent() {
        return groupLevel;
    }

    @Override public Collection<DataStructureSizeNode> childes() {
        throw new UnsupportedOperationException();
    }

    @Override public DataStructureSizeNode createChild(Void context) {
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
