package org.apache.ignite.internal.pagemem;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.INDEX;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.INDEX_REUSE_LIST;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.INDEX_TREE;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PARTITION;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.REUSE_LIST;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.TOTAL;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.simpleTracker;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.sizeWithTrackingPages;

public class DataStructureSizeNodeGroupLevel implements DataStructureSizeNode<String, DataStructureSizeNodePartitionLevel> {
    private final DataStructureSizeNode parent;

    private final String name;

    private final Map<String, DataStructureSizeNode> parts = new ConcurrentLinkedHashMap<>();

    private final Map<String, DataStructureSize> structures = new ConcurrentLinkedHashMap<>();

    private final int pageSize = 0;

    public DataStructureSizeNodeGroupLevel(DataStructureSizeNode parent, String name) {
        this.parent = parent;
        this.name = name;

        DataStructureSize indexesTreePages = simpleTracker(name + "-" + INDEX_TREE);
        DataStructureSize indexesReuseListPages = simpleTracker(name + "-" + INDEX_REUSE_LIST);

        structures.put(indexesTreePages.name(), indexesTreePages);
        structures.put(indexesReuseListPages.name(), indexesReuseListPages);

        DataStructureSize pkIndexPages = simpleTracker(name + "-" + PK_INDEX);
        DataStructureSize reuseListPages = simpleTracker(name + "-" + REUSE_LIST);
        DataStructureSize dataPages = simpleTracker(name + "-" + DATA);
        DataStructureSize pureDataSize = simpleTracker(name + "-" + PURE_DATA);

        structures.put(pkIndexPages.name(), pkIndexPages);
        structures.put(reuseListPages.name(), reuseListPages);
        structures.put(dataPages.name(), dataPages);
        structures.put(pureDataSize.name(), pureDataSize);

        // Internal size.
        DataStructureSize internalSize = simpleTracker(name + "-" + INTERNAL);

        structures.put(internalSize.name(), internalSize);

        // Index size.
        DataStructureSize indexTotalPages = sizeWithTrackingPages(name + "-" + INDEX, internalSize, pageSize);

        structures.put(indexTotalPages.name(), indexTotalPages);

        // Partitions size.
        DataStructureSize partitionTotalPages = simpleTracker(name + "-" + PARTITION);

        structures.put(partitionTotalPages.name(), partitionTotalPages);

        // Total size.
        DataStructureSize totalPages = new DataStructureSizeAdapter() {
            @Override public long size() {
                return (indexTotalPages.size() + partitionTotalPages.size()) * pageSize + internalSize.size();
            }

            @Override public String name() {
                return name + "-" + TOTAL;
            }
        };

        structures.put(totalPages.name(), totalPages);
    }

    @Override public DataStructureSizeNode parent() {
        return null;
    }

    @Override public Collection<DataStructureSizeNode> childes() {
        return parts.values();
    }

    @Override public Collection<DataStructureSize> structures() {
        return structures.values();
    }

    @Override public DataStructureSizeNodePartitionLevel createChild(String part) {
        DataStructureSizeNodePartitionLevel partLevel = new DataStructureSizeNodePartitionLevel(this, part);

        parts.put(partLevel.name(), partLevel);

        return partLevel;
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return structures.get(name + "-" + structure);
    }

    @Override public String name() {
        return name;
    }
}
