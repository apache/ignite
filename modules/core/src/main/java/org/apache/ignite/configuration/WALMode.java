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

package org.apache.ignite.configuration;

import org.jetbrains.annotations.Nullable;

/**
 * WAL Mode. This enum defines crash recovery guarantees when Ignite persistence is enabled.
 */
public enum WALMode {
    /**
     * Default mode: full-sync disk writes. These writes survive power loss scenarios.
     */
    DEFAULT,

    /**
     * Log only mode: flushes application buffers. These writes survive process crash.
     */
    LOG_ONLY,

    /**
     * Background mode. Does not force application buffer flush. Data may be lost in case of process crash.
     */
    BACKGROUND,

    /**
     * WAL disabled.
     */
    NONE;

    /**
     * Enumerated values.
     */
    private static final WALMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable
    public static WALMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}