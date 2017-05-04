package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface PageIdIterable extends Iterable<FullPageId> {
    /** */
    public boolean contains(FullPageId fullId) throws IgniteCheckedException;

    /** */
    @Nullable public FullPageId next(@Nullable FullPageId fullId);

    /** */
    public double progress(FullPageId curr);

    /**
     *
     */
    public boolean isEmpty();
}
