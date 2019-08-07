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

package org.apache.ignite.internal.visor.verify;

import org.jetbrains.annotations.Nullable;

/**
 * Represents a type of cache(s) that can be used for comparing update counters and checksums between primary and backup partitions.
 * <br>
 * @see org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2
 */
public enum CacheFilterEnum {
    /** Default - user only, or all caches specified by name. */
    DEFAULT,

    /** System. */
    SYSTEM,

    /** Persistent. */
    PERSISTENT,

    /** Not persistent. */
    NOT_PERSISTENT,

    /** User. */
    USER,

    /** All. */
    ALL;

    /** Enumerated values. */
    private static final CacheFilterEnum[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    public static @Nullable CacheFilterEnum fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
