/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cluster;

import org.jetbrains.annotations.Nullable;

/**
 * Cluster states.
 */
public enum ClusterState {
    /**
     * Cluster deactivated. Cache operations aren't allowed.
     * <p>
     * <b>NOTE:</b>
     * Deactivation clears in-memory caches (without persistence) including the system caches.
     */
    INACTIVE(false),

    /** Cluster activated. All cache operations are allowed. */
    ACTIVE(true),

    /** Cluster activated. Cache read operation allowed, Cache data change operation aren't allowed. */
    ACTIVE_READ_ONLY(true);

    /** Cluster activated flag. */
    private final boolean active;

    /** */
    ClusterState(boolean active) {
        this.active = active;
    }

    /**
     * @return {@code True} if cluster activated in this state and {@code false} otherwise.
     */
    public boolean active() {
        return active;
    }

    /** Enumerated values. */
    private static final ClusterState[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static ClusterState fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
