/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.plugin.security;

import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Supported security subject types. Subject type can be retrieved form {@link SecuritySubject#type()} method.
 */
public enum SecuritySubjectType {
    /**
     * Subject type for a remote {@link ClusterNode}.
     */
    REMOTE_NODE,

    /**
     * Subject type for remote client.
     */
    REMOTE_CLIENT;

    /** Enumerated values. */
    private static final SecuritySubjectType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static SecuritySubjectType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}