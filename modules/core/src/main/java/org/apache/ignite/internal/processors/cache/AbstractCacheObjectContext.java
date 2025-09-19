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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of {@link CacheObjectValueContext}.
 */
public abstract class AbstractCacheObjectContext implements CacheObjectValueContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     */
    protected AbstractCacheObjectContext(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public BinaryContext binaryContext() {
        return ctx.cacheObjects().binaryContext();
    }

    /** {@inheritDoc} */
    @Override public void waitMetadataWriteIfNeeded(final int typeId) {
        ctx.cacheObjects().waitMetadataWriteIfNeeded(typeId);
    }

    /** {@inheritDoc} */
    @Override public @Nullable ClassLoader classLoader() {
        return ctx.config().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader globalLoader() {
        return ctx.cache().context().deploy().globalLoader();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return ctx.log(cls);
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object val) throws IgniteCheckedException {
        return ctx.cacheObjects().marshal(this, val);
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        return ctx.cacheObjects().unmarshal(this, bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public boolean isPeerClassLoadingEnabled() {
        return ctx.config().isPeerClassLoadingEnabled();
    }
}
