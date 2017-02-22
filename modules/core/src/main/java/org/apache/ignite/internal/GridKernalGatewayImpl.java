/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
@GridToStringExclude
public class GridKernalGatewayImpl implements GridKernalGateway, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringExclude
    private final ReadWriteLock rwLock =
        new StripedCompositeReadWriteLock(Runtime.getRuntime().availableProcessors());

    /** */
    @GridToStringExclude
    private IgniteFutureImpl<?> reconnectFut;

    /** */
    private final AtomicReference<GridKernalState> state = new AtomicReference<>(GridKernalState.STOPPED);

    /** */
    @GridToStringExclude
    private final String gridName;

    /**
     * User stack trace.
     *
     * Intentionally uses non-volatile variable for optimization purposes.
     */
    private String stackTrace;

    /**
     * @param gridName Grid name.
     */
    public GridKernalGatewayImpl(String gridName) {
        this.gridName = gridName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "BusyWait"})
    @Override public void readLock() throws IllegalStateException {
        if (stackTrace == null)
            stackTrace = stackTrace();

        Lock lock = rwLock.readLock();

        lock.lock();

        GridKernalState state = this.state.get();

        if (state != GridKernalState.STARTED) {
            // Unlock just acquired lock.
            lock.unlock();

            if (state == GridKernalState.DISCONNECTED) {
                assert reconnectFut != null;

                throw new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected: " + gridName);
            }

            throw illegalState();
        }
    }

    /** {@inheritDoc} */
    @Override public void readLockAnyway() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        rwLock.readLock().lock();

        if (state.get() == GridKernalState.DISCONNECTED)
            throw new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected: " + gridName);
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        rwLock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait"})
    @Override public void writeLock() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true)
            try {
                if (rwLock.writeLock().tryLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /** {@inheritDoc} */
    @Override public boolean tryWriteLock(long timeout) throws InterruptedException {
        boolean acquired = rwLock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS);

        if (acquired) {
            if (stackTrace == null)
                stackTrace = stackTrace();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> onDisconnected() {
        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        reconnectFut = new IgniteFutureImpl<>(fut);

        if (!state.compareAndSet(GridKernalState.STARTED, GridKernalState.DISCONNECTED)) {
            ((GridFutureAdapter<?>)reconnectFut.internalFuture()).onDone(new IgniteCheckedException("Node stopped."));

            return null;
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        if (state.compareAndSet(GridKernalState.DISCONNECTED, GridKernalState.STARTED))
            ((GridFutureAdapter<?>)reconnectFut.internalFuture()).onDone();
    }

    /**
     * Retrieves user stack trace.
     *
     * @return User stack trace.
     */
    private static String stackTrace() {
        StringWriter sw = new StringWriter();

        new Throwable().printStackTrace(new PrintWriter(sw));

        return sw.toString();
    }

    /**
     * Creates new illegal state exception.
     *
     * @return Newly created exception.
     */
    private IllegalStateException illegalState() {
        return new IllegalStateException("Grid is in invalid state to perform this operation. " +
            "It either not started yet or has already being or have stopped [gridName=" + gridName +
            ", state=" + state + ']');
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock() {
        rwLock.writeLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public void setState(GridKernalState state) {
        assert state != null;

        // NOTE: this method should always be called within write lock.
        this.state.set(state);

        if (reconnectFut != null)
            ((GridFutureAdapter<?>)reconnectFut.internalFuture()).onDone(new IgniteCheckedException("Node stopped."));
    }

    /** {@inheritDoc} */
    @Override public GridKernalState getState() {
        return state.get();
    }

    /** {@inheritDoc} */
    @Override public String userStackTrace() {
        return stackTrace;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridKernalGatewayImpl.class, this);
    }
}
