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

import org.apache.ignite.IgniteCheckedException;
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
    private KeyCacheObject key;

    /** */
    @Order(value = 1, method = "version")
    @GridToStringInclude
    private GridCacheVersion ver;

    /** */
    @Order(2)
    private boolean near;

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

    /**
     * @return Key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @param key New key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver New version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @return {@code True} if key should be evicted from near cache.
     */
    public boolean near() {
        return near;
    }

    /**
     * @param near {@code True} if key should be evicted from near cache.
     */
    public void near(boolean near) {
        this.near = near;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 97;
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        key.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        key.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }
}
