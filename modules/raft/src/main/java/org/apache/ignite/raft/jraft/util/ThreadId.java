/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replicator id with lock.
 */
public class ThreadId {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadId.class);

    private static final int TRY_LOCK_TIMEOUT_MS = 10;

    private final Object data;
    private final NonReentrantLock lock = new NonReentrantLock();
    private final List<Integer> pendingErrors = Collections.synchronizedList(new ArrayList<>());
    private final OnError onError;
    private volatile boolean destroyed;

    /**
     *
     */
    public interface OnError {

        /**
         * Error callback, it will be called in lock, but should take care of unlocking it.
         *
         * @param id the thread id
         * @param data the data
         * @param errorCode the error code
         */
        void onError(final ThreadId id, final Object data, final int errorCode);
    }

    public ThreadId(final Object data, final OnError onError) {
        super();
        this.data = data;
        this.onError = onError;
        this.destroyed = false;
    }

    public boolean isDestroyed() {
        return this.destroyed;
    }

    // TODO asch why Object here ? IGNITE-14832
    public Object getData() {
        return this.data;
    }

    public Object lock() {
        if (this.destroyed) {
            return null;
        }
        try {
            while (!this.lock.tryLock(TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                if (this.destroyed) {
                    return null;
                }
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); // reset
            return null;
        }
        // Got the lock, double checking state.
        if (this.destroyed) {
            // should release lock
            this.lock.unlock();
            return null;
        }
        return this.data;
    }

    public void unlock() {
        if (!this.lock.isHeldByCurrentThread()) {
            LOG.warn("Fail to unlock with {}, the lock is held by {} and current thread is {}.", this.data,
                this.lock.getOwner(), Thread.currentThread());
            return;
        }
        // calls all pending errors before unlock
        boolean doUnlock = true;
        try {
            final List<Integer> errors;
            synchronized (this.pendingErrors) {
                errors = new ArrayList<>(this.pendingErrors);
                this.pendingErrors.clear();
            }
            for (final Integer code : errors) {
                // The lock will be unlocked in onError.
                doUnlock = false;
                if (this.onError != null) {
                    this.onError.onError(this, this.data, code);
                }
            }
        }
        finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
    }

    public void join() {
        while (!this.destroyed) {
            ThreadHelper.onSpinWait();
        }
    }

    @Override
    public String toString() {
        return this.data.toString();
    }

    public void unlockAndDestroy() {
        if (this.destroyed) {
            return;
        }
        this.destroyed = true;
        if (!this.lock.isHeldByCurrentThread()) {
            LOG.warn("Fail to unlockAndDestroy with {}, the lock is held by {} and current thread is {}.", this.data,
                this.lock.getOwner(), Thread.currentThread());
            return;
        }
        this.lock.unlock();
    }

    /**
     * Set error code, if it tryLock success, run the onError callback with code immediately, else add it into pending
     * errors and will be called before unlock.
     *
     * @param errorCode error code
     */
    public void setError(final int errorCode) {
        if (this.destroyed) {
            return;
        }
        if (this.lock.tryLock()) {
            if (this.destroyed) {
                this.lock.unlock();
                return;
            }
            if (this.onError != null) {
                // The lock will be unlocked in onError.
                this.onError.onError(this, this.data, errorCode);
            }
        }
        else {
            this.pendingErrors.add(errorCode);
        }
    }
}
