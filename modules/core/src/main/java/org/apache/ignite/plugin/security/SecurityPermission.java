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

package org.apache.ignite.plugin.security;

import org.jetbrains.annotations.*;

/**
 * Supported security permissions within grid. Permissions
 * are specified on per-cache or per-task level.
 */
public enum SecurityPermission {
    /** Cache {@code read} permission. */
    CACHE_READ,

    /** Cache {@code put} permission. */
    CACHE_PUT,

    /** Cache {@code remove} permission. */
    CACHE_REMOVE,

    /** Task {@code execute} permission. */
    TASK_EXECUTE,

    /** Task {@code cancel} permission. */
    TASK_CANCEL,

    /** Events {@code enable} permission. */
    EVENTS_ENABLE,

    /** Events {@code disable} permission. */
    EVENTS_DISABLE,

    /** Common visor view tasks permission. */
    ADMIN_VIEW,

    /** Visor cache read (query) permission. */
    ADMIN_QUERY,

    /** Visor cache load permission. */
    ADMIN_CACHE,

    /** Visor admin operations permissions. */
    ADMIN_OPS;

    /** Enumerated values. */
    private static final SecurityPermission[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static SecurityPermission fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
