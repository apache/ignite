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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteException;

/**
 * Contains cancellation closure.
 */
public class GridQueryCancel {
    /** No op. static closure. Used for representing cancelled state. */
    private static final Runnable NO_OP = new Runnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Cancellation closure. */
    private volatile Runnable clo;

    /** Cancellable state flag. */
    private boolean cancellable;

    /** Updater. */
    private final AtomicReferenceFieldUpdater<GridQueryCancel, Runnable> updater =
        AtomicReferenceFieldUpdater.newUpdater(GridQueryCancel.class, Runnable.class, "clo");

    /** Cancel requested. */
    private volatile boolean cancelRequested;

    /**
     * @param clo Clo.
     */
    public void set(Runnable clo) {
        this.clo = clo;
    }

    /**
     * Waits for cancellable state.
     */
    public synchronized void waitForCancellableState() {
        while (!cancellable)
            try {
                wait();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteException("Interrtuped while waiting for cancellable state.");
            }
    }

    /**
     * Enters cancellable state notifying all waiters.
     */
    public synchronized void enterCancellableState() {
        cancellable = true;

        notifyAll();
    }

    /**
     * Leaves cancellable state.
     */
    public synchronized void leaveCancellableState() {
        cancellable = false;
    }

    /**
     * Tries cancelling a query. If query is already completed or cancelled, this is no-op.
     */
    public void cancel() {
        Runnable clo0 = clo;

        if (clo0 != null && updater.compareAndSet(this, clo0, NO_OP)) {
            cancelRequested = true;

            waitForCancellableState();

            clo.run();
        }
    }

    /**
     * @return Cancel requested.
     */
    public boolean cancelRequested() {
        return cancelRequested;
    }
}