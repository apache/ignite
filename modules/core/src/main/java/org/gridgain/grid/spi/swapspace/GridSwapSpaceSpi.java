/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace;

import org.apache.ignite.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.spi.swapspace.noop.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Provides a mechanism in grid for storing data on disk. GridGain cache uses swap space to overflow
 * data to disk if it cannot fit in memory. It's also possible to use swap space directly
 * by calling {@link org.apache.ignite.Ignite} API swap-related methods. Logically storage is organized into
 * independent 'spaces' in which data is stored.
 * <p>
 * All swap space implementations can be configured to prevent infinite growth and evict oldest entries.
 * <p>
 * The default swap space SPI is {@link GridFileSwapSpaceSpi} which stores values on disk in files and keeps keys in
 * memory.
 * <p>
 * Gridgain provides the following {@code GridSwapSpaceSpi} implementations:
 * <ul>
 * <li>
 *     {@link GridFileSwapSpaceSpi} - pure Java implementation with in-memory keys. This SPI is used by default.
 * </li>
 * <li>
 *     {@link GridNoopSwapSpaceSpi} - no-op SPI mainly for testing.
 * </li>
 * </ul>
 * <p>
 * <p>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface GridSwapSpaceSpi extends IgniteSpi {
    /**
     * Entirely clears data space with given name, if any.
     *
     * @param spaceName Space name to clear.
     * @throws GridSpiException In case of any errors.
     */
    public void clear(@Nullable String spaceName) throws GridSpiException;

    /**
     * Gets size in bytes for data space with given name. If specified space does
     * not exist this method returns {@code 0}.
     *
     * @param spaceName Space name to get size for.
     * @return Size in bytes.
     * @throws GridSpiException In case of any errors.
     */
    public long size(@Nullable String spaceName) throws GridSpiException;

    /**
     * Gets number of stored entries (keys) in data space with given name. If specified
     * space does not exist this method returns {@code 0}.
     *
     * @param spaceName Space name to get number of entries for.
     * @return Number of stored entries in specified space.
     * @throws GridSpiException In case of any errors.
     */
    public long count(@Nullable String spaceName) throws GridSpiException;

    /**
     * Reads stored value as array of bytes by key from data space with given name.
     * If specified space does not exist this method returns {@code null}.
     *
     * @param spaceName Name of the data space to read from.
     * @param key Key used to read value from data space.
     * @param ctx Swap context.
     * @return Value as array of bytes stored in specified data space that matches
     *      to given key.
     * @throws GridSpiException In case of any errors.
     */
    @Nullable public byte[] read(@Nullable String spaceName, GridSwapKey key, GridSwapContext ctx)
        throws GridSpiException;

    /**
     * Reads stored values as array of bytes by all passed keys from data space with
     * given name. If specified space does not exist this method returns empty map.
     *
     * @param spaceName Name of the data space to read from.
     * @param keys Keys used to read values from data space.
     * @param ctx Swap context.
     * @return Map in which keys are the ones passed into method and values are
     *      corresponding values read from swap storage.
     * @throws GridSpiException In case of any errors.
     */
    public Map<GridSwapKey, byte[]> readAll(@Nullable String spaceName,
        Iterable<GridSwapKey> keys, GridSwapContext ctx) throws GridSpiException;

    /**
     * Removes value stored in data space with given name corresponding to specified key.
     *
     * @param spaceName Space name to remove value from.
     * @param key Key to remove value in the specified space for.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is not executed.
     * @param ctx Swap context.
     * @throws GridSpiException In case of any errors.
     */
    public void remove(@Nullable String spaceName, GridSwapKey key,
        @Nullable IgniteInClosure<byte[]> c, GridSwapContext ctx) throws GridSpiException;

    /**
     * Removes values stored in data space with given name corresponding to specified keys.
     *
     * @param spaceName Space name to remove values from.
     * @param keys Keys to remove value in the specified space for.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is not executed.
     * @param ctx Swap context.
     * @throws GridSpiException In case of any errors.
     */
    public void removeAll(@Nullable String spaceName, Collection<GridSwapKey> keys,
        @Nullable IgniteBiInClosure<GridSwapKey, byte[]> c, GridSwapContext ctx) throws GridSpiException;

    /**
     * Stores value as array of bytes with given key into data space with given name.
     *
     * @param spaceName Space name to store key-value pair into.
     * @param key Key to store given value for. This key can be used further to
     *      read or remove stored value.
     * @param val Some value as array of bytes to store into specified data space.
     * @param ctx Swap context.
     * @throws GridSpiException In case of any errors.
     */
    public void store(@Nullable String spaceName, GridSwapKey key, @Nullable byte[] val, GridSwapContext ctx)
        throws GridSpiException;

    /**
     * Stores key-value pairs (both keys and values are arrays of bytes) into data
     * space with given name.
     *
     * @param spaceName Space name to store key-value pairs into.
     * @param pairs Map of stored key-value pairs where each one is an array of bytes.
     * @param ctx Swap context.
     * @throws GridSpiException In case of any errors.
     */
    public void storeAll(@Nullable String spaceName, Map<GridSwapKey, byte[]> pairs, GridSwapContext ctx)
        throws GridSpiException;

    /**
     * Sets eviction listener to receive notifications on evicted swap entries.
     *
     * @param evictLsnr Eviction listener ({@code null} to stop receiving notifications).
     */
    public void setListener(@Nullable GridSwapSpaceSpiListener evictLsnr);

    /**
     * Gets partitions IDs that are stored in the passed in space.
     *
     * @param spaceName Space name.
     * @return Partitions IDs or {@code null} if space is unknown.
     * @throws GridSpiException If failed.
     */
    @Nullable public Collection<Integer> partitions(@Nullable String spaceName) throws GridSpiException;

    /**
     * Gets iterator over space keys.
     *
     * @param spaceName Space name.
     * @param ctx Swap context.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws GridSpiException If failed.
     */
    @Nullable <K> GridSpiCloseableIterator<K> keyIterator(@Nullable String spaceName, GridSwapContext ctx)
        throws GridSpiException;

    /**
     * Gets raw iterator over space entries.
     *
     * @param spaceName Space name.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws GridSpiException If failed.
     */
    @Nullable public GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName)
        throws GridSpiException;

    /**
     * Gets raw iterator over space entries.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws GridSpiException If failed.
     */
    @Nullable public GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName,
        int part) throws GridSpiException;
}
