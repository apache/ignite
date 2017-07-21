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

package org.apache.ignite.internal.processors.cache.extras;

import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Cache extras.
 */
public interface GridCacheEntryExtras {
    /**
     * @return MVCC.
     */
    @Nullable public GridCacheMvcc mvcc();

    /**
     * @param mvcc NVCC.
     * @return Updated extras.
     */
    public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc);

    /**
     * @return Obsolete version.
     */
    @Nullable public GridCacheVersion obsoleteVersion();

    /**
     * @param obsoleteVer Obsolete version.
     * @return Updated extras.
     */
    public GridCacheEntryExtras obsoleteVersion(GridCacheVersion obsoleteVer);

    /**
     * @return TTL.
     */
    public long ttl();

    /**
     * @return Expire time.
     */
    public long expireTime();

    /**
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @return Updated extras.
     */
    public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime);

    /**
     * @return Extras size.
     */
    public int size();
}