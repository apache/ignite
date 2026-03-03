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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class CacheEvictionEntry implements Message {
    /** */
    @Order(0)
    @GridToStringInclude
    KeyCacheObject key;

    /** */
    @Order(1)
    @GridToStringInclude
    GridCacheVersion ver;

    /** */
    @Order(2)
    boolean near;

    /**
     * Default constructor.
     */
    public CacheEvictionEntry() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param ver Version.
     * @param near {@code true} if key should be evicted from near cache.
     */
    public CacheEvictionEntry(KeyCacheObject key, GridCacheVersion ver, boolean near) {
        this.key = key;
        this.ver = ver;
        this.near = near;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 97;
    }

}
