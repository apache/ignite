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

package org.apache.ignite.fs;

import org.jetbrains.annotations.*;

/**
 * {@code GGFS} mode defining interactions with underlying secondary Hadoop file system.
 * Secondary Hadoop file system is provided for pass-through, write-through, and
 * read-through purposes.
 * <p>
 * This mode is configured via {@link IgniteFsConfiguration#getDefaultMode()}
 * configuration property.
 */
public enum IgniteFsMode {
    /**
     * In this mode GGFS will not delegate to secondary Hadoop file system and will
     * cache all the files in memory only.
     */
    PRIMARY,

    /**
     * In this mode GGFS will not cache any files in memory and will only pass them
     * through to secondary Hadoop file system. If this mode is enabled, then
     * secondary Hadoop file system must be configured.
     *
     * @see IgniteFsConfiguration#getSecondaryHadoopFileSystemUri()
     */
    PROXY,

    /**
     * In this mode {@code GGFS} will cache files locally and also <i>synchronously</i>
     * write them through to secondary Hadoop file system.
     * <p>
     * If secondary Hadoop file system is not configured, then this mode behaves like
     * {@link #PRIMARY} mode.
     *
     * @see IgniteFsConfiguration#getSecondaryHadoopFileSystemUri()
     */
    DUAL_SYNC,

    /**
     * In this mode {@code GGFS} will cache files locally and also <i>asynchronously</i>
     * write them through to secondary Hadoop file system.
     * <p>
     * If secondary Hadoop file system is not configured, then this mode behaves like
     * {@link #PRIMARY} mode.
     *
     * @see IgniteFsConfiguration#getSecondaryHadoopFileSystemUri()
     */
    DUAL_ASYNC;

    /** Enumerated values. */
    private static final IgniteFsMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static IgniteFsMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
