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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;

/**
 * Cache projection flags that specify projection behaviour. This flags can be explicitly passed into
 * the following methods on {@link CacheProjection}:
 * <ul>
 * <li>{@link CacheProjection#flagsOn(CacheFlag...)}</li>
 * <li>{@link CacheProjection#flagsOff(CacheFlag...)}</li>
 * </ul>
 */
public enum CacheFlag {
    /**
     * Clone values prior to returning them to user.
     * <p>
     * Whenever values are returned from cache, they cannot be directly updated
     * as cache holds the same references internally. If it is needed to
     * update values that are returned from cache, this flag will provide
     * automatic cloning of values prior to returning so they can be directly
     * updated.
     */
    CLONE,

    /** Skips store, i.e. no read-through and no write-through behavior. */
    SKIP_STORE;

    /** */
    private static final CacheFlag[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheFlag fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
