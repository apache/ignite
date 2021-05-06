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
    /** Cache id. */
    private final int cacheId;

    /** Cache name. */
    private final String cacheName;

    /** Saved in MetaStorage. */
    private final boolean saved;

    /** Index rebuild is complete. */
    private boolean completed;

    /**
     * Constructor.
     *
     * @param cacheId Cache id.
     * @param cacheName Cache name.
     * @param saved Saved in MetaStorage.
     */
    public IndexRebuildState(int cacheId, String cacheName, boolean saved) {
        this.cacheId = cacheId;
        this.cacheName = cacheName;
        this.saved = saved;
    }

    /**
     * Getting cache id.
     *
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * Getting cache name.
     *
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Checking whether it is stored in the MetaStorage.
     *
     * @return {@code True} if stored in the MetaStorage.
     */
    public boolean saved() {
        return saved;
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
