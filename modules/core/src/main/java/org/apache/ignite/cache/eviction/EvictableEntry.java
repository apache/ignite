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

package org.apache.ignite.cache.eviction;

import org.jetbrains.annotations.*;

import javax.cache.*;

/**
 * Evictable cache entry passed into {@link org.apache.ignite.cache.eviction.GridCacheEvictionPolicy}.
 *
 * @author @java.author
 * @version @java.version
 */
public interface EvictableEntry extends Cache.Entry {
    /**
     * Attaches metadata to the entry.
     *
     * @param meta Metadata to attach. Pass {@code null} to remove previous value.
     * @return Previous metadata value.
     */
    public <T> T attachMeta(@Nullable Object meta);

    /**
     * Replaces entry metadata.
     *
     * @param oldMeta Old metadata value, possibly {@code null}.
     * @param newMeta New metadata value, possibly {@code null}.
     * @return {@code True} if metadata value was replaced.
     */
    public boolean replaceMeta(@Nullable Object oldMeta, @Nullable Object newMeta);
}
