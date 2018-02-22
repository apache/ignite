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
public class EntryGetWithTtlResult extends EntryGetResult {
    /** */
    private final long expireTime;

    /** */
    private final long ttl;

    /**
     * @param val Value.
     * @param ver Version.
     * @param reserved Reserved flag.
     * @param expireTime Entry expire time.
     * @param ttl Entry time to live.
     */
    public EntryGetWithTtlResult(Object val, GridCacheVersion ver, boolean reserved, long expireTime, long ttl) {
        super(val, ver, reserved);
        this.expireTime = expireTime;
        this.ttl = ttl;
    }

    /**
     * @return Entry expire time.
     */
    @Override public long expireTime() {
        return expireTime;
    }

    /**
     * @return Entry time to live.
     */
    @Override public long ttl() {
        return ttl;
    }
}
