// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

/**
 * This interface provides a rich API for working with distributedly cached atomic long value.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic long includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets current value of atomic long.
 * </li>
 * <li>
 * Various {@code get..(..)} methods synchronously get current value of atomic long
 * and increase or decrease value of atomic long.
 * </li>
 * <li>
 * Method {@link #addAndGet(long l)} synchronously sums {@code l} with current value of atomic long
 * and returns result.
 * </li>
 * <li>
 * Method {@link #incrementAndGet()} synchronously increases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #decrementAndGet()} synchronously decreases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #getAndSet(long l)} synchronously gets current value of atomic long and sets {@code l}
 * as value of atomic long.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #name()} gets name of atomic long.
 * </li>
 * </ul>
 * <p>
 * <h1 class="header">Creating Distributed Atomic Long</h1>
 * Instance of distributed atomic long can be created by calling the following method:
 * <ul>
 *     <li>{@link GridCacheDataStructures#atomicLong(String, long, boolean)}</li>
 * </ul>
 *
 * @author @java.author
 * @version @java.version
 * @see GridCacheDataStructures#atomicLong(String, long, boolean)
 * @see GridCacheDataStructures#removeAtomicLong(String)
 */
public interface GridCacheAtomicLong extends GridMetadataAware{
    /**
     * Name of atomic long.
     *
     * @return Name of atomic long.
     */
    public String name();

    /**
     * Gets current value of atomic long.
     *
     * @return Current value of atomic long.
     * @throws GridException If operation failed.
     */
    public long get() throws GridException;

    /**
     * Gets current value of atomic long asynchronously.
     *
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> getAsync() throws GridException;

    /**
     * Increments and gets current value of atomic long.
     *
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long incrementAndGet() throws GridException;

    /**
     * Increments and gets current value of atomic long asynchronously.
     *
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> incrementAndGetAsync() throws GridException;

    /**
     * Gets and increments current value of atomic long.
     *
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long getAndIncrement() throws GridException;

    /**
     * Gets and increments current value of atomic long asynchronously.
     *
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> getAndIncrementAsync() throws GridException;

    /**
     * Adds {@code l} and gets current value of atomic long.
     *
     * @param l Number which will be added.
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long addAndGet(long l) throws GridException;

    /**
     * Adds {@code l} and gets current value of atomic long asynchronously.
     *
     * @param l Number which will be added.
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> addAndGetAsync(long l) throws GridException;

    /**
     * Gets current value of atomic long and adds {@code l}.
     *
     * @param l Number which will be added.
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long getAndAdd(long l) throws GridException;

    /**
     * Gets current value of atomic long and adds {@code l} asynchronously.
     *
     * @param l Number which will be added.
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> getAndAddAsync(long l) throws GridException;

    /**
     * Decrements and gets current value of atomic long.
     *
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long decrementAndGet() throws GridException;

    /**
     * Decrements and gets current value of atomic long asynchronously.
     *
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> decrementAndGetAsync() throws GridException;

    /**
     * Gets and decrements current value of atomic long.
     *
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long getAndDecrement() throws GridException;

    /**
     * Gets and decrements current value of atomic long asynchronously.
     *
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> getAndDecrementAsync() throws GridException;

    /**
     * Gets current value of atomic long and sets new value {@code l} of atomic long.
     *
     * @param l New value of atomic long.
     * @return Value.
     * @throws GridException If operation failed.
     */
    public long getAndSet(long l) throws GridException;

    /**
     * Gets current value of atomic long and sets new value {@code l} of atomic long asynchronously.
     *
     * @param l New value of atomic long.
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Long> getAndSetAsync(long l) throws GridException;

    /**
     * Atomically sets the new value {@code l} of atomic long if set of predicates is true asynchronously.
     *
     * @param l New value of atomic long.
     * @param p Predicate which should evaluate to {@code true} for value to be set.
     * @param pa Additional predicates can be used optional.
     * @return Result of predicates execution. If {@code true} value of atomic long will be updated.
     * @throws GridException If operation failed.
     */
    public boolean compareAndSet(long l, GridPredicate<Long> p, GridPredicate<Long>... pa) throws GridException;

    /**
     * Atomically sets the new value {@code l} of atomic long if set of predicates is true.
     *
     * @param l New value of atomic long.
     * @param p Predicate which should evaluate to {@code true} for value to be set.
     * @param ps Additional predicates can be used optional.
     * @return Future that completes once calculation has finished.
     * @throws GridException If operation failed.
     */
    public GridFuture<Boolean> compareAndSetAsync(long l, GridPredicate<Long> p, GridPredicate<Long>... ps)
        throws GridException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean removed();
}
