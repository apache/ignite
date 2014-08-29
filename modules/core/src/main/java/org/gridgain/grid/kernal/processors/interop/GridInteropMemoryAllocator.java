/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop;

/**
 * Interop memory allocator.
 */
public interface GridInteropMemoryAllocator {
    /**
     * Allocate managed memory.
     *
     * @param size Desired size.
     * @return Allocated array.
     */
    public byte[] allocate(int size);

    /**
     * Allocate direct memory.
     *
     * @param size Desired size.
     * @return Address.
     */
    public long allocateDirect(int size);

    /**
     * Release one direct memory region and allocate another.
     *
     * @param addr Address to release.
     * @param size Desired size.
     * @return Address.
     */
    public long releaseAndAllocateDirect(long addr, int size);

    /**
     * Release direct memory region.
     *
     * @param addr Address.
     */
    public void release(long addr);
}
