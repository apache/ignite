package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;

/**
 * Page IO that supports compaction.
 */
public interface CompactablePageIO {
    /**
     * Compacts page contents to the output buffer.
     * Implementation must not change position and limit of the original page buffer.
     *
     * @param page Page buffer.
     * @param out Output buffer.
     */
    void compactPage(ByteBuffer page, ByteBuffer out);

    /**
     * Restores the original page in place.
     *
     * @param compactPage Compact page.
     * @param pageSize Page size.
     */
    void restorePage(ByteBuffer compactPage, int pageSize);
}
