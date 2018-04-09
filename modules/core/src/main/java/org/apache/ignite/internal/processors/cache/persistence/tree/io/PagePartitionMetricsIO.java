package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INDEX_REUSE_LIST;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INDEX_TREE;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PARTITION;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PK_INDEX;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.REUSE_LIST;

public class PagePartitionMetricsIO extends PageIO {
    /** */
    public static final IOVersions<PagePartitionMetricsIO> VERSIONS = new IOVersions<>(
        new PagePartitionMetricsIO(1)
    );

    /** */
    private static final int PARTITION_SIZE = PageIO.COMMON_HEADER_END;

    /** */
    private static final int PK_INDEX_SIZE = PARTITION_SIZE + 8;

    /** */
    private static final int REUSE_LIST_SIZE = PK_INDEX_SIZE + 8;

    /** */
    private static final int DATA_SIZE = REUSE_LIST_SIZE + 8;

    /** */
    private static final int PURE_DATA_SIZE = DATA_SIZE + 8;

    /** */
    private static final int INTERNAL_SIZE = PURE_DATA_SIZE + 8;

    /** */
    private static final int INDEX_SIZE = INTERNAL_SIZE + 8;

    /** */
    private static final int INDEX_REUSE_LIST_SIZE = INDEX_SIZE + 8;

    /** */
    private static final int INDEX_TREE_SIZE = INDEX_REUSE_LIST_SIZE + 8;

    /**
     * @param ver Page format version.
     */
    protected PagePartitionMetricsIO(int ver) {
        super(T_PARTITION_METRICS, ver);
    }

    /**
     *
     */
    public long getPartitionSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, PARTITION_SIZE);
    }

    /**
     *
     */
    public void setPartitionSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, PARTITION_SIZE, size);
    }

    /**
     *
     */
    public long getPKIndexSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, PK_INDEX_SIZE);
    }

    /**
     *
     */
    public void setPKIndexSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, PK_INDEX_SIZE, size);
    }

    /**
     *
     */
    public long getReuseListSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, REUSE_LIST_SIZE);
    }

    /**
     *
     */
    public void setReuseListSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, REUSE_LIST_SIZE, size);
    }

    /**
     *
     */
    public long getDataSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, DATA_SIZE);
    }

    /**
     *
     */
    public void setDataSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, DATA_SIZE, size);
    }

    /**
     *
     */
    public long getPureDataSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, PURE_DATA_SIZE);
    }

    /**
     *
     */
    public void setPureDataSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, PURE_DATA_SIZE, size);
    }

    /**
     *
     */
    public long getInternalSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, INTERNAL_SIZE);
    }

    /**
     *
     */
    public void setInternalSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, INTERNAL_SIZE, size);
    }

    /**
     *
     */
    public long getIndexSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, INDEX_SIZE);
    }

    /**
     *
     */
    public void setIndexSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, INDEX_SIZE, size);
    }

    /**
     *
     */
    public long getIndexReuseListSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, INDEX_REUSE_LIST_SIZE);
    }

    /**
     *
     */
    public void setIndexReuseListSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, INDEX_REUSE_LIST_SIZE, size);
    }

    /**
     *
     */
    public long getIndexTreeSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, INDEX_TREE_SIZE);
    }

    /**
     *
     */
    public void setIndexTreeSize(long pageAddr, long size) {
        PageUtils.putLong(pageAddr, INDEX_TREE_SIZE, size);
    }

    @Override protected void printPage(
        long addr,
        int pageSize,
        GridStringBuilder sb
    ) throws IgniteCheckedException {
        sb.a("PagePartitionMetricsIO[\n")
            .a(PARTITION).a("=").a(getPartitionSize(addr)).a("\n").
            a(PK_INDEX).a("=").a(getPKIndexSize(addr)).a("\n").
            a(REUSE_LIST).a("=").a(getReuseListSize(addr)).a("\n").
            a(DATA).a("=").a(getDataSize(addr)).a("\n").
            a(PURE_DATA).a("=").a(getPureDataSize(addr)).a("\n").
            a(INTERNAL).a("=").a(getInternalSize(addr)).a("\n").
            a(INDEX).a("=").a(getIndexSize(addr)).a("\n").
            a(INDEX_REUSE_LIST).a("=").a(getIndexReuseListSize(addr)).a("\n").
            a(INDEX_TREE).a("=").a(getIndexTreeSize(addr)).a("\n")
            .a("]\n");
    }
}
