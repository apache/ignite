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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Entry for batch swap operations.
 */
public class GridCacheBatchSwapEntry<K, V> extends GridCacheSwapEntryImpl<V> {
    /** Key. */
    private K key;

    /** Key bytes. */
    private byte[] keyBytes;

    /** Partition. */
    private int part;

    /**
     * Creates batch swap entry.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param part Partition id.
     * @param valBytes Value bytes.
     * @param valIsByteArr Whether value is byte array.
     * @param ver Version.
     * @param ttl Time to live.
     * @param expireTime Expire time.
     * @param keyClsLdrId Key class loader ID.
     * @param valClsLdrId Optional value class loader ID.
     */
    public GridCacheBatchSwapEntry(K key,
        byte[] keyBytes,
        int part,
        ByteBuffer valBytes,
        boolean valIsByteArr,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        IgniteUuid keyClsLdrId,
        @Nullable IgniteUuid valClsLdrId) {
        super(valBytes, valIsByteArr, ver, ttl, expireTime, keyClsLdrId, valClsLdrId);

        this.key = key;
        this.keyBytes = keyBytes;
        this.part = part;
    }

    /**
     * @return Key.
     */
    public K key() {
        return key;
    }

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes() {
        return keyBytes;
    }

    /**
     * @return Partition id.
     */
    public int partition() {
        return part;
    }
}
