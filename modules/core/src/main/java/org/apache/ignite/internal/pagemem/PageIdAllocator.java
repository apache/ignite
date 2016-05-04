package org.apache.ignite.internal.pagemem;

import org.apache.ignite.IgniteCheckedException;

/**
 * Allocates page ID's.
 */
public interface PageIdAllocator {
    /** */
    public static final byte FLAG_META = 0;

    /** */
    public static final byte FLAG_DATA = 1;

    /** */
    public static final byte FLAG_IDX = 2;

    /**
     * Allocates a page from the space for the given partition ID and the given flags.
     *
     * @param partId Partition ID.
     * @param flags Flags to separate page spaces.
     * @return Allocated page ID.
     */
    public FullPageId allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException;

    /**
     * The given page is free now.
     *
     * @param pageId Full page ID.
     */
    public boolean freePage(FullPageId pageId) throws IgniteCheckedException;
}
