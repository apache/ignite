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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Cache object and version, told apart by the key they belong to. */
public class KeyedVersionedValue extends CacheVersionedValue {
    /** */
    @Order(0)
    @GridToStringInclude
    KeyCacheObject key;

    /** */
    public KeyedVersionedValue() {
        // No-op.
    }

    /** */
    public KeyedVersionedValue(IgniteTxKey txKey, CacheObject val, GridCacheVersion ver) {
        super(val, ver, txKey.cacheId());

        key = txKey.key();
    }

    /** @return Key this value belongs to. */
    public IgniteTxKey txKey() {
        return new IgniteTxKey(key, cacheId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(KeyedVersionedValue.class, this, "super", super.toString());
    }
}
