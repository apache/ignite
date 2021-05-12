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

/**
 * State of rebuilding indexes for the cache.
 */
public class IndexRebuildState {
    /** Persistent cache. */
    private final boolean persistent;

    /** Index rebuild is complete. */
    private boolean completed;

    /**
     * Constructor.
     *
     * @param persistent Persistent cache.
     */
    public IndexRebuildState(boolean persistent) {
        this.persistent = persistent;
    }

    /**
     * Checking if the cache is persistent.
     *
     * @return {@code True} if persistent.
     */
    public boolean persistent() {
        return persistent;
    }

    /**
     * Checks if rebuilding indexes for the cache has completed.
     *
     * @return {@code True} if completed.
     */
    public boolean completed() {
        return completed;
    }

    /**
     * Changing the state of completion of rebuilding cache indexes.
     *
     * @param completed Completion state.
     * @return {@code this} for chaining.
     */
    public IndexRebuildState completed(boolean completed) {
        this.completed = completed;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexRebuildState.class, this);
    }
}
