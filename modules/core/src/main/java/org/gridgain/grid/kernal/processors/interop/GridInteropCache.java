/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop;

/**
 * Interop cache wrapper.
 */
public interface GridInteropCache {
    /**
     * Get value from cache.
     *
     * @param keyAddr Address where key is stored in unmanaged memory.
     * @return Address where value is stored in unmanaged memory.
     */
    public long get(long keyAddr);

    /**
     * Get value from cache.
     *
     * @param keyAddr Address where key is stored in unmanaged memory.
     * @param cbAddr Callback address.
     * @return {@code 0} in case of success, positive value representing
     *     unmanaged memory address in case of exception.
     */
    public long getAsync(long keyAddr, long cbAddr);

    /**
     * Put value to cache.
     *
     * @param keyValAddr Address where key and value are stored in unmanaged memory.
     * @return {@code 0} in case of success, positive value representing unmanaged
     *     memory address in case of exception.
     */
    public long put(long keyValAddr);
}
