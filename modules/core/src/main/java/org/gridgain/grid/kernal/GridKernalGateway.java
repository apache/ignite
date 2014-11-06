/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.util.tostring.*;

/**
 * This interface guards access to implementations of public methods that access kernal
 * functionality from the following main API interfaces:
 * <ul>
 * <li>{@link GridProjection}</li>
 * </ul>
 * Note that this kernal gateway <b>should not</b> be used to guard against method from
 * the following non-rich interfaces since their implementations are already managed
 * by their respective implementing classes:
 * <ul>
 * <li>{@link Grid}</li>
 * <li>{@link GridNode}</li>
 * </ul>
 * Kernal gateway is also responsible for notifying various futures about the change in
 * kernal state so that issued futures could properly interrupt themselves when kernal
 * becomes unavailable while future is held externally by the user.
 */
@GridToStringExclude
public interface GridKernalGateway {
    /**
     * Performs light-weight check on the kernal state at the moment of this call.
     * <p>
     * This method should only be used when the kernal state should be checked just once
     * at the beginning of the method and the fact that <b>kernal state can change in the middle
     * of such method's execution</b> should not matter.
     * <p>
     * For example, when a method returns a constant value its implementation doesn't depend
     * on the kernal being valid throughout its execution. In such case it is enough to check
     * the kernal's state just once at the beginning of this method to provide consistent behavior
     * of the API without incurring overhead of <code>lock-based</code> guard methods.
     *
     * @throws IllegalStateException Thrown in case when no kernal calls are allowed.
     */
    public void lightCheck() throws IllegalStateException;

    /**
     * Should be called on entering every kernal related call
     * <b>originated directly or indirectly via public API</b>.
     * <p>
     * This method essentially acquires a read lock and multiple threads
     * can enter the call without blocking.
     *
     * @throws IllegalStateException Thrown in case when no kernal calls are allowed.
     * @see #readUnlock()
     */
    public void readLock() throws IllegalStateException;

    /**
     * Sets kernal state. Various kernal states drive the logic inside of the gateway.
     *
     * @param state Kernal state to set.
     */
    public void setState(GridKernalState state);

    /**
     * Gets current kernal state.
     *
     * @return Kernal state.
     */
    public GridKernalState getState();

    /**
     * Should be called on leaving every kernal related call
     * <b>originated directly or indirectly via public API</b>.
     * <p>
     * This method essentially releases the internal read-lock acquired previously
     * by {@link #readLock()} method.
     *
     * @see #readLock()
     */
    public void readUnlock();

    /**
     * This method waits for all current calls to exit and blocks any further
     * {@link #readLock()} calls until {@link #writeUnlock()} method is called.
     * <p>
     * This method essentially acquires the internal write lock.
     */
    public void writeLock();

    /**
     * This method unblocks {@link #writeLock()}.
     * <p>
     * This method essentially releases internal write lock previously acquired
     * by {@link #writeLock()} method.
     */
    public void writeUnlock();

    /**
     * Adds stop listener. Note that the identity set will be used to store listeners for
     * performance reasons. Futures can register a listener to be notified when they need to
     * be internally interrupted.
     *
     * @param lsnr Listener to add.
     */
    public void addStopListener(Runnable lsnr);

    /**
     * Removes previously added stop listener.
     *
     * @param lsnr Listener to remove.
     */
    public void removeStopListener(Runnable lsnr);

    /**
     * Gets user stack trace through the first call of grid public API.
     */
    public String userStackTrace();
}

