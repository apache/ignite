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

package org.apache.ignite.compute;

import org.jetbrains.annotations.Nullable;

/**
 * Defines life-time scopes for checkpoint operations. Such operations include:
 * <ul>
 *      <li>{@link ComputeTaskSession#saveCheckpoint(String, Object, ComputeTaskSessionScope, long)}</li>
 * </ul>
 */
public enum ComputeTaskSessionScope {
    /**
     * Data saved with this scope will be automatically removed
     * once the task session is completed (i.e. execution of the task is completed)
     * or when they time out. This is the most often used scope for checkpoints and swap space.
     * It provides behavior for use case when jobs can failover on other nodes
     * within the same session and thus checkpoints or data saved to swap space should be
     * preserved for the duration of the entire session.
     */
    SESSION_SCOPE,

    /**
     * Data saved with this scope will only be removed automatically
     * if they time out and time out is supported. Currently, only checkpoints support timeouts.
     * Any data, however, can always be removed programmatically via methods on {@link ComputeTaskSession}
     * interface.
     */
    GLOBAL_SCOPE;

    /** Enumerated values. */
    private static final ComputeTaskSessionScope[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static ComputeTaskSessionScope fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}