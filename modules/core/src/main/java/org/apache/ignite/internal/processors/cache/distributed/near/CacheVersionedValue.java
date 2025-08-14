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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Cache object and version.
 */
public class CacheVersionedValue implements Message {
    /** Value. */
    @Order(value = 0, method = "value")
    @GridToStringInclude
    private CacheObject val;

    /** Cache version. */
    @Order(value = 1, method = "version")
    @GridToStringInclude
    private GridCacheVersion ver;

    /** */
    public CacheVersionedValue() {
        // No-op.
    }

    /**
     * @param val Cache value.
     * @param ver Cache version.
     */
    public CacheVersionedValue(CacheObject val, GridCacheVersion ver) {
        this.val = val;
        this.ver = ver;
    }

    /**
     * @return Cache version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver New cache version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @return Cache object.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @param val New value.
     */
    public void value(CacheObject val) {
        this.val = val;
    }

    /**
     * This method is called before the whole message is sent
     * and is responsible for pre-marshalling state.
     *
     * @param ctx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        if (val != null)
            val.prepareMarshal(ctx);
    }

    /**
     * This method is called after the whole message is received
     * and is responsible for unmarshalling state.
     *
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (val != null)
            val.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 102;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheVersionedValue.class, this);
    }
}
