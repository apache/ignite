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
 * Disk page compression options.
 *
 * @see CacheConfiguration#setDiskPageCompression
 * @see CacheConfiguration#setDiskPageCompressionLevel
 * @see DataStorageConfiguration#setWalPageCompression
 * @see DataStorageConfiguration#setWalPageCompressionLevel
 */
public enum DiskPageCompression {
    /** Compression disabled. */
    DISABLED,

    /** Retain only useful data from half-filled pages, but do not apply any compression. */
    SKIP_GARBAGE,

    /** Zstd compression. */
    ZSTD,

    /** LZ4 compression. */
    LZ4,

    /** Snappy compression. */
    SNAPPY;

    /** Enumerated values. */
    private static final DiskPageCompression[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static DiskPageCompression fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
