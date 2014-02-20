// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

/**
 * Factory to create smart pointer instances.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridOffHeapSmartPointerFactory<T extends GridOffHeapSmartPointer> {
    /**
     * @param ptr Pointer.
     * @return Smart pointer instance (may or may not return the same instance multiple times).
     */
    public T createPointer(long ptr);
}
