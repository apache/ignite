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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class DataStreamerEntry implements Map.Entry<KeyCacheObject, CacheObject>, Message {
    /** */
    @Order(value = 0, method = "keyCacheObject")
    @GridToStringInclude
    protected KeyCacheObject key;

    /** */
    @Order(value = 1, method = "valueCacheObject")
    @GridToStringInclude
    protected CacheObject val;

    /**
     *
     */
    public DataStreamerEntry() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public DataStreamerEntry(KeyCacheObject key, CacheObject val) {
        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public CacheObject getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public CacheObject setValue(CacheObject val) {
        CacheObject old = this.val;

        this.val = val;

        return old;
    }

    /**
     * @param ctx Cache context.
     * @return Map entry unwrapping internal key and value.
     */
    public <K, V> Map.Entry<K, V> toEntry(final GridCacheContext ctx, final boolean keepBinary) {
        return new Map.Entry<K, V>() {
            @Override public K getKey() {
                return (K)ctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false, null);
            }

            @Override public V setValue(V val) {
                throw new UnsupportedOperationException();
            }

            @Override public V getValue() {
                return (V)ctx.cacheObjectContext().unwrapBinaryIfNeeded(val, keepBinary, false, null);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 95;
    }

    /**
     * @return Key.
     */
    public KeyCacheObject keyCacheObject() {
        return key;
    }

    /**
     * @param key New key.
     */
    public void keyCacheObject(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Value.
     */
    public CacheObject valueCacheObject() {
        return val;
    }

    /**
     * @param val New value.
     */
    public void valueCacheObject(CacheObject val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerEntry.class, this);
    }
}
