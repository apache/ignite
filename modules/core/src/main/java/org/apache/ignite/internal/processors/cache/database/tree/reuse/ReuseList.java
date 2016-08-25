package org.apache.ignite.internal.processors.cache.database.tree.reuse;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.DataStructure;

/**
 * Reuse list.
 */
public interface ReuseList {

    /**
     * @param pageId Page ID to recycle.
     */
    public void add(long pageId) throws IgniteCheckedException;

    /**
     * @param bag Reuse bag.
     * @throws IgniteCheckedException If failed.
     */
    public void add(ReuseBag bag) throws IgniteCheckedException;

    /**
     * TODO drop client
     *
     * @param client Client.
     * @param bag Reuse bag.
     * @return Page ID or {@code 0} if none available.
     * @throws IgniteCheckedException If failed.
     */
    public long take(DataStructure client, ReuseBag bag) throws IgniteCheckedException;

    /**
     * @return Size in pages.
     * @throws IgniteCheckedException If failed.
     */
    public long size() throws IgniteCheckedException;
}
