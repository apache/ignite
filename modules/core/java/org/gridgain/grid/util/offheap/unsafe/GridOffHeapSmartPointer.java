/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

/**
 * Smart pointer with reference counting.
 */
public interface GridOffHeapSmartPointer {
    /**
     * @return Pointer address.
     */
    public long pointer();

    /**
     * Increment reference count.
     */
    public void incrementRefCount();

    /**
     * Decrement reference count.
     */
    public void decrementRefCount();
}
