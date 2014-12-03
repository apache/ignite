/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;

/**
 * This interface provides a rich API for working with distributed atomic sequence.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic sequence includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets current value from atomic sequence.
 * </li>
 * <li>
 * Various {@code get..(..)} methods synchronously get current value from atomic sequence
 * and increase atomic sequences value.
 * </li>
 * <li>
 * Various {@code add..(..)} {@code increment(..)} methods synchronously increase atomic sequences value
 * and return increased value.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #batchSize(int size)} sets batch size of current atomic sequence.
 * </li>
 * <li>
 * Method {@link #batchSize()} gets current batch size of atomic sequence.
 * </li>
 * <li>
 * Method {@link #name()} gets name of atomic sequence.
 * </li>
 * </ul>
 * <h1 class="header">Creating Distributed Atomic Sequence</h1>
 * Instance of distributed atomic sequence can be created by calling the following method:
 * <ul>
 *     <li>{@link GridCacheDataStructures#atomicSequence(String, long, boolean)}</li>
 * </ul>
 * @see GridCacheDataStructures#atomicSequence(String, long, boolean)
 * @see GridCacheDataStructures#removeAtomicSequence(String)
 */
public interface IgniteAtomicSequence {
    /**
     * Name of atomic sequence.
     *
     * @return Name of atomic sequence.
     */
    public String name();

    /**
     * Gets current value of atomic sequence.
     *
     * @return Value of atomic sequence.
     * @throws GridException If operation failed.
     */
    public long get() throws GridException;

    /**
     * Increments and returns the value of atomic sequence.
     *
     * @return Value of atomic sequence after increment.
     * @throws GridException If operation failed.
     */
    public long incrementAndGet() throws GridException;

    /**
     * Gets and increments current value of atomic sequence.
     *
     * @return Value of atomic sequence before increment.
     * @throws GridException If operation failed.
     */
    public long getAndIncrement() throws GridException;

    /**
     * Adds {@code l} elements to atomic sequence and gets value of atomic sequence.
     *
     * @param l Number of added elements.
     * @return Value of atomic sequence.
     * @throws GridException If operation failed.
     */
    public long addAndGet(long l) throws GridException;

    /**
     * Gets current value of atomic sequence and adds {@code l} elements.
     *
     * @param l Number of added elements.
     * @return Value of atomic sequence.
     * @throws GridException If operation failed.
     */
    public long getAndAdd(long l) throws GridException;

    /**
     * Gets local batch size for this atomic sequence.
     *
     * @return Sequence batch size.
     */
    public int batchSize();

    /**
     * Sets local batch size for atomic sequence.
     *
     * @param size Sequence batch size. Must be more then 0.
     */
    public void batchSize(int size);

    /**
     * Gets status of atomic sequence.
     *
     * @return {@code true} if atomic sequence was removed from cache, {@code false} otherwise.
     */
    public boolean removed();
}
