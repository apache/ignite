/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

/**
 * Compound memory object. Contains one or more memory regions.
 */
public interface GridUnsafeCompoundMemory {
    /**
     * Deallocates this compound memory object.
     */
    public void deallocate();

    /**
     * Merges another compound memory object with this one.
     *
     * @param compound Compound memory.
     */
    public void merge(GridUnsafeCompoundMemory compound);
}
