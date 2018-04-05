package org.apache.ignite.internal.pagemem;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;

import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.simpleTracker;

public class DataStructureSizeManager {
    /** */
    public static final String TOTAL = "total";

    /** */
    public static final String INDEX = "indexes";

    /** */
    public static final String INDEX_TREE = "indexes-tree";

    /** */
    public static final String INDEX_REUSE_LIST = "indexes-reuse-list";

    /** */
    public static final String PARTITION = "partition";

    /** */
    public static final String PK_INDEX = "pk-index";

    /** */
    public static final String REUSE_LIST = "reuse-list";

    /** */
    public static final String DATA = "data";

    /** */
    public static final String PURE_DATA = "pure-data";

    /** */
    public static final String INTERNAL = "internal";

    /** */
    private final Map<String, DataStructureSize> sizes = new LinkedHashMap<>();

    /** */
    private final String grpName;

    private final int pageSize;

    /**
     * @param grpCtx Cache group context.
     */
    public DataStructureSizeManager(CacheGroupContext grpCtx) {
        grpName = grpCtx.cacheOrGroupName();

        pageSize = grpCtx.dataRegion().pageMemory().pageSize();

        String indexesTree = grpName + "-" + INDEX_TREE;
        String indexesReuseList = grpName + "-" + INDEX_REUSE_LIST;

        DataStructureSize indexesTreePages = simpleTracker(indexesTree);
        DataStructureSize indexesReuseListPages = simpleTracker(indexesReuseList);

        sizes.put(indexesTree, indexesTreePages);
        sizes.put(indexesReuseList, indexesReuseListPages);

        String pkIndex = grpName + "-" + PK_INDEX;
        String reuseList = grpName + "-" + REUSE_LIST;
        String data = grpName + "-" + DATA;
        String pureData = grpName + "-" + PURE_DATA;

        DataStructureSize pkIndexPages = simpleTracker(pkIndex);
        DataStructureSize reuseListPages = simpleTracker(reuseList);
        DataStructureSize dataPages = simpleTracker(data);
        DataStructureSize pureDataSize = simpleTracker(pureData);

        sizes.put(pkIndex, pkIndexPages);
        sizes.put(reuseList, reuseListPages);
        sizes.put(data, dataPages);
        sizes.put(pureData, pureDataSize);

        // Internal size.
        String internal = grpName + "-" + INTERNAL;

        DataStructureSize internalSize = simpleTracker(internal);

        sizes.put(internal, internalSize);

        // Index size.
        String indexesTotal = grpName + "-" + INDEX;

        DataStructureSize indexTotalPages = simpleTracker(indexesTotal);

        sizes.put(indexesTotal, indexTotalPages);

        // Partitions size.
        String partitionTotal = grpName + "-" + PARTITION;

        DataStructureSize partitionTotalPages = simpleTracker(partitionTotal);

        sizes.put(partitionTotal, partitionTotalPages);

        // Total size.
        final String total = grpName + "-" + TOTAL;

        DataStructureSize totalPages = new DataStructureSizeAdapter() {
            @Override public long size() {
                return (indexTotalPages.size() + partitionTotalPages.size()) * pageSize + internalSize.size();
            }

            @Override public String name() {
                return total;
            }
        };

        sizes.put(total, totalPages);
    }

    private DataStructureSize dataStructureSize(String name) {
        return sizes.get(grpName + "-" + name);
    }

    public Map<String, DataStructureSize> structureSizes() {
        return sizes;
    }
}
