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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class EntryGetResult {
    /** */
    private Object val;

    /** */
    private final GridCacheVersion ver;

    /** */
    private final boolean reserved;

    /**
     * @param val Value.
     * @param ver Version.
     * @param reserved Reserved flag.
     */
    public EntryGetResult(Object val, GridCacheVersion ver, boolean reserved) {
        this.val = val;
        this.ver = ver;
        this.reserved = reserved;
    }

    /**
     * @param val Value.
     * @param ver Version.
     */
    public EntryGetResult(Object val, GridCacheVersion ver) {
        this(val, ver, false);
    }

    /**
     * @return Value.
     */
    public <T> T value() {
        return (T)val;
    }

    /**
     * @param val Value.
     */
    public void value(Object val) {
        this.val = val;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Reserved flag.
     */
    public boolean reserved() {
        return reserved;
    }

    /**
     * @return Entry expire time.
     */
    public long expireTime() {
        return 0L;
    }

    /**
     * @return Entry time to live.
     */
    public long ttl() {
        return 0L;
    }
}
