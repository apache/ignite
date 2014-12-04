/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

/**
 * This interface provides a rich API for working with distributed atomic stamped value.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic stamped includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets both value and stamp of atomic.
 * </li>
 * <li>
 * Method {@link #value()} synchronously gets current value of atomic.
 * </li>
 * <li>
 * Method {@link #stamp()} synchronously gets current stamp of atomic.
 * </li>
 * <li>
 * Method {@link #set(Object, Object)} synchronously and unconditionally sets the value
 * and the stamp in the atomic.
 * </li>
 * <li>
 * Methods {@code compareAndSet(...)} synchronously and conditionally set the value
 * and the stamp in the atomic.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #name()} gets name of atomic stamped.
 * </li>
 * </ul>
 * <h1 class="header">Creating Distributed Atomic Stamped</h1>
 * Instance of distributed atomic stamped can be created by calling the following method:
 * <ul>
 *     <li>{@link GridCacheDataStructures#atomicLong(String, long, boolean)}</li>
 * </ul>
 * @see GridCacheDataStructures#atomicStamped(String, Object, Object, boolean)
 * @see GridCacheDataStructures#removeAtomicStamped(String)
 */
public interface GridCacheAtomicStamped<T, S> {
    /**
     * Name of atomic stamped.
     *
     * @return Name of atomic stamped.
     */
    public String name();

    /**
     * Gets both current value and current stamp of atomic stamped.
     *
     * @return both current value and current stamp of atomic stamped.
     * @throws GridException If operation failed.
     */
    public IgniteBiTuple<T, S> get() throws GridException;

    /**
     * Unconditionally sets the value and the stamp.
     *
     * @param val Value.
     * @param stamp Stamp.
     * @throws GridException If operation failed.
     */
    public void set(T val, S stamp) throws GridException;

    /**
     * Conditionally sets the new value and new stamp. They will be set if {@code expVal}
     * and {@code expStamp} are equal to current value and current stamp respectively.
     *
     * @param expVal Expected value.
     * @param newVal New value.
     * @param expStamp Expected stamp.
     * @param newStamp New stamp.
     * @return Result of operation execution. If {@code true} than  value and stamp will be updated.
     * @throws GridException If operation failed.
     */
    public boolean compareAndSet(T expVal, T newVal, S expStamp, S newStamp) throws GridException;

    /**
     * Gets current stamp.
     *
     * @return Current stamp.
     * @throws GridException If operation failed.
     */
    public S stamp() throws GridException;

    /**
     * Gets current value.
     *
     * @return Current value.
     * @throws GridException If operation failed.
     */
    public T value() throws GridException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic stamped was removed from cache, {@code false} otherwise.
     */
    public boolean removed();
}
