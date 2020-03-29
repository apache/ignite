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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheObjectImpl extends CacheObjectAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public CacheObjectImpl() {
        // No-op.
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public CacheObjectImpl(Object val, byte[] valBytes) {
        assert val != null || valBytes != null;

        this.val = val;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
        cpy = cpy && needCopy(ctx);

        try {
            GridKernalContext kernalCtx = ctx.kernalContext();

            IgniteCacheObjectProcessor proc = ctx.kernalContext().cacheObjects();

            if (cpy) {
                if (valBytes == null) {
                    assert val != null;

                    valBytes = proc.marshal(ctx, val);
                }

                ClassLoader clsLdr;

                if (val != null)
                    clsLdr = val.getClass().getClassLoader();
                else if (kernalCtx.config().isPeerClassLoadingEnabled())
                    clsLdr = kernalCtx.cache().context().deploy().globalLoader();
                else
                    clsLdr = null;

                return (T)proc.unmarshal(ctx, valBytes, clsLdr);
            }

            if (val != null)
                return (T)val;

            assert valBytes != null;

            Object val = proc.unmarshal(ctx, valBytes, kernalCtx.config().isPeerClassLoadingEnabled() ?
                kernalCtx.cache().context().deploy().globalLoader() : null);

            if (ctx.storeValue())
                this.val = val;

            return (T)val;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unmarshall object.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (valBytes == null)
            valBytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (val == null && ctx.storeValue())
            val = ctx.kernalContext().cacheObjects().unmarshal(ctx, valBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 89;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert false;

        return super.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        assert false;

        return super.equals(obj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return this;
    }
}
