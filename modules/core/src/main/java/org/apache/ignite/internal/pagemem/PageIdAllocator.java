package org.apache.ignite.internal.pagemem;

/**
 * Allocates page ID's.
 */
public interface PageIdAllocator {
    /** */
    public static final byte FLAG_DATA = 1;

    /** */
    public static final byte FLAG_IDX = 2;

    /** */
    public static final byte FLAG_META = 4;

    /**
     * TODO do we need a generic abstraction for flags?
     * Allocates a page from the space for the given partition ID and the given flags.
     *
     * @param partId Partition ID.
     * @param flags Flags to separate page spaces.
     * @return Allocated page ID.
     */
    public long allocatePage(int cacheId, int partId, byte flags);

    /**
     * The given page is free now.
     *
     * @param pageId Page ID.
     * @return
     */
    public boolean freePage(long pageId);
}
