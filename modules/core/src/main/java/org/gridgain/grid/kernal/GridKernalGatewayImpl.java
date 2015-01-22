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

package org.gridgain.grid.kernal;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.GridKernalState.*;

/**
 *
 */
@GridToStringExclude
public class GridKernalGatewayImpl implements GridKernalGateway, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringExclude
    private final GridSpinReadWriteLock rwLock = new GridSpinReadWriteLock();

    /** */
    @GridToStringExclude
    private final Collection<Runnable> lsnrs = new GridSetWrapper<>(new IdentityHashMap<Runnable, Object>());

    /** */
    private volatile GridKernalState state = STOPPED;

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
    @Override public void lightCheck() throws IllegalStateException {
        if (state != STARTED)
            throw illegalState();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "BusyWait"})
    @Override public void readLock() throws IllegalStateException {
        if (stackTrace == null)
            stackTrace = stackTrace();

        rwLock.readLock();

        if (state != STARTED) {
            // Unlock just acquired lock.
            rwLock.readUnlock();

            throw illegalState();
        }

        enterThreadLocals();
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        leaveThreadLocals();

        rwLock.readUnlock();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait"})
    @Override public void writeLock() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        enterThreadLocals();

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true)
            try {
                if (rwLock.tryWriteLock(200, TimeUnit.MILLISECONDS))
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

    /**
     * Enter thread locals.
     */
    private void enterThreadLocals() {
        GridThreadLocal.enter();
        GridThreadLocalEx.enter();
    }

    /**
     * Leave thread locals.
     */
    private void leaveThreadLocals() {
        GridThreadLocalEx.leave();
        GridThreadLocal.leave();
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock() {
        rwLock.writeUnlock();

        leaveThreadLocals();
    }

    /** {@inheritDoc} */
    @Override public void setState(GridKernalState state) {
        assert state != null;

        // NOTE: this method should always be called within write lock.
        this.state = state;

        if (state == STOPPING) {
            Runnable[] runs;

            synchronized (lsnrs) {
                lsnrs.toArray(runs = new Runnable[lsnrs.size()]);
            }

            // In the same thread.
            for (Runnable r : runs)
                r.run();
        }
    }

    /** {@inheritDoc} */
    @Override public GridKernalState getState() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public void addStopListener(Runnable lsnr) {
        assert lsnr != null;

        if (state == STARTING || state == STARTED)
            synchronized (lsnrs) {
                lsnrs.add(lsnr);
            }
        else
            // Call right away in the same thread.
            lsnr.run();
    }

    /** {@inheritDoc} */
    @Override public void removeStopListener(Runnable lsnr) {
        assert lsnr != null;

        synchronized (lsnrs) {
            lsnrs.remove(lsnr);
        }
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
