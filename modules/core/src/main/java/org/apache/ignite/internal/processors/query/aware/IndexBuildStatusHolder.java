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

package org.apache.ignite.internal.processors.query.aware;

import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.COMPLETE;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.DELETE;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.INIT;

/**
 * Cache index build status.
 * The following operations affect the status: rebuilding indexes and building a new index.
 * <p/>
 * If the operation starts executing, then the status is {@link Status#INIT INIT}.
 * If all operations are completed, then the status is {@link Status#COMPLETE COMPLETED}.
 * Status {@link Status#DELETE DELETE} is used for persistent cache to mark at the
 * beginning of a checkpoint that all operations have been completed and they will be committed to it.
 */
public class IndexBuildStatusHolder {
    /** Enumeration of statuses. */
    public enum Status {
        /** Initial status - operation(s) in progress. */
        INIT,

        /** All operations complete. */
        COMPLETE,

        /** To be deleted. */
        DELETE
    }

    /** Persistent cache. */
    private final boolean persistent;

    /** Status. Guarded by {@code this}. */
    private Status status;

    /** Rebuilding indexes. Guarded by {@code this}. */
    private boolean rebuild;

    /** Count of new indexes being built. Guarded by {@code this}. */
    private int newIdx;

    /**
     * Constructor.
     *
     * @param persistent Persistent cache.
     * @param rebuild {@code True} if rebuilding indexes, otherwise building a new index.
     */
    public IndexBuildStatusHolder(boolean persistent, boolean rebuild) {
        this.persistent = persistent;

        onStartOperation(rebuild);
    }

    /**
     * Callback on the start of of the operation.
     *
     * @param rebuild {@code True} if rebuilding indexes, otherwise building a new index.
     * @see #onFinishOperation
     */
    public synchronized void onStartOperation(boolean rebuild) {
        status = INIT;

        if (rebuild)
            this.rebuild = true;
        else {
            assert newIdx >= 0;

            newIdx++;
        }
    }

    /**
     * Callback on the finish of the operation.
     *
     * @param rebuild {@code True} if rebuilding indexes, otherwise building a new index.
     * @return Current status.
     * @see #onStartOperation
     */
    public synchronized Status onFinishOperation(boolean rebuild) {
        if (rebuild)
            this.rebuild = false;
        else {
            assert newIdx > 0;

            newIdx--;
        }

        if (!this.rebuild && newIdx == 0)
            status = COMPLETE;

        return status;
    }

    /**
     * Change of status to {@link Status#DELETE} if the current status is {@link Status#COMPLETE}.
     * Note that this will only be for persistent cache.
     *
     * @return {@code True} if successful.
     */
    public boolean delete() {
        if (persistent) {
            synchronized (this) {
                if (status == COMPLETE) {
                    status = DELETE;

                    return true;
                }
                else
                    return false;
            }
        }
        else
            return false;
    }

    /**
     * Getting the current status.
     *
     * @return Current status.
     */
    public synchronized Status status() {
        return status;
    }

    /**
     * Checking whether rebuilding indexes is in progress.
     *
     * @return {@code True} if in progress.
     */
    public synchronized boolean rebuild() {
        return rebuild;
    }

    /**
     * Getting the count of new indexes that are currently being built.
     *
     * @return Count of new indexes being built.
     */
    public synchronized int buildNewIndexes() {
        return newIdx;
    }

    /**
     * Checking if the cache is persistent.
     *
     * @return {@code True} if persistent.
     */
    public boolean persistent() {
        return persistent;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(IndexBuildStatusHolder.class, this);
    }
}
