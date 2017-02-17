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

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class EntryGetResult {
    /** */
    private final CacheObject val;

    /** */
    private final GridCacheVersion ver;

    /** */
    private final boolean reserved;

    /**
     * @param val Value.
     * @param ver Version.
     */
    EntryGetResult(CacheObject val, GridCacheVersion ver, boolean reserved) {
        this.val = val;
        this.ver = ver;
        this.reserved = reserved;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Reserved flag,
     */
    public boolean reserved() {
        return reserved;
    }
}
