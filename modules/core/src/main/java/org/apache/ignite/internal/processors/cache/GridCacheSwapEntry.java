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

import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

/**
 * Swap entry.
 */
public interface GridCacheSwapEntry {
    /**
     * @return Value bytes.
     */
    public byte[] valueBytes();

    /**
     * @return Object type.
     */
    public byte type();

    /**
     * @param valBytes Value bytes.
     */
    public void valueBytes(@Nullable byte[] valBytes);

    /**
     * @return Value.
     */
    public CacheObject value();

    /**
     * @param val Value.
     */
    public void value(CacheObject val);

    /**
     * @return Version.
     */
    public GridCacheVersion version();

    /**
     * @return Time to live.
     */
    public long ttl();

    /**
     * @return Expire time.
     */
    public long expireTime();

    /**
     * @return Class loader ID for entry key ({@code null} for local class loader).
     */
    @Nullable public IgniteUuid keyClassLoaderId();

    /**
     * @return Class loader ID for entry value ({@code null} for local class loader).
     */
    @Nullable public IgniteUuid valueClassLoaderId();

    /**
     * @return If entry is offheap based returns offheap address, otherwise 0.
     */
    public long offheapPointer();
}
