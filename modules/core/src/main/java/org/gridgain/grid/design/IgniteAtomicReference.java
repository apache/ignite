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
 * This interface provides a rich API for working with distributed atomic reference.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed atomic reference includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} synchronously gets current value of an atomic reference.
 * </li>
 * <li>
 * Method {@link #set(Object)} synchronously and unconditionally sets the value in the an atomic reference.
 * </li>
 * <li>
 * Methods {@code compareAndSet(...)} synchronously and conditionally set the value in the an atomic reference.
 * </li>
 * </ul>
 * All previously described methods have asynchronous analogs.
 * <ul>
 * <li>
 * Method {@link #name()} gets name of atomic reference.
 * </li>
 * </ul>
 * <h1 class="header">Creating Distributed Atomic Reference</h1>
 * Instance of distributed atomic reference can be created by calling the following method:
 * <ul>
 *     <li>{@link GridCacheDataStructures#atomicReference(String, Object, boolean)}</li>
 * </ul>
 * @see GridCacheDataStructures#atomicReference(String, Object, boolean)
 * @see GridCacheDataStructures#removeAtomicReference(String)
 */
public interface IgniteAtomicReference<T> {
    /**
     * Name of atomic reference.
     *
     * @return Name of an atomic reference.
     */
    public String name();

    /**
     * Gets current value of an atomic reference.
     *
     * @return current value of an atomic reference.
     * @throws GridException If operation failed.
     */
    public T get() throws GridException;

    /**
     * Unconditionally sets the value.
     *
     * @param val Value.
     * @throws GridException If operation failed.
     */
    public void set(T val) throws GridException;

    /**
     * Conditionally sets the new value. That will be set if {@code expVal} is equal
     * to current value respectively.
     *
     * @param expVal Expected value.
     * @param newVal New value.
     * @return Result of operation execution. If {@code true} than value have been updated.
     * @throws GridException If operation failed.
     */
    public boolean compareAndSet(T expVal, T newVal) throws GridException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if an atomic reference was removed from cache, {@code false} otherwise.
     */
    public boolean removed();
}
