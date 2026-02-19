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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.CacheObjectUnsafeUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.CacheObjectUtils.objectPutSize;

/**
 *
 */
public abstract class CacheObjectAdapter implements CacheObject, Externalizable {
    /** */
    private static final long serialVersionUID = 2006765505127197251L;

    /** */
    @GridToStringInclude(sensitive = true)
    protected Object val;

    /** */
    protected byte[] valBytes;

    /**
     * @param ctx Context.
     * @return {@code True} need to copy value returned to user.
     */
    protected boolean needCopy(CacheObjectValueContext ctx) {
        return ctx.copyOnGet() && val != null && !BinaryUtils.immutable(val);
    }

    /**
     * @return Value bytes from value.
     */
    protected byte[] valueBytesFromValue(CacheObjectValueContext ctx) throws IgniteCheckedException {
        byte[] bytes = ctx.marshal(val);

        return ctx.transformIfNecessary(bytes);
    }

    /**
     * @return Value from value bytes.
     */
    protected Object valueFromValueBytes(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        byte[] bytes = ctx.restoreIfNecessary(valBytes);

        return ctx.unmarshal(bytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        return TYPE_REGULAR;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert valBytes != null;

        U.writeByteArray(out, valBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        valBytes = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
        assert valBytes != null : "Value bytes must be initialized before object is stored";

        return putValue(buf, 0, objectPutSize(valBytes.length));
    }

    /** {@inheritDoc} */
    @Override public int putValue(long addr) throws IgniteCheckedException {
        assert valBytes != null : "Value bytes must be initialized before object is stored";

        return CacheObjectUnsafeUtils.putValue(addr, cacheObjectType(), valBytes);
    }

    /** {@inheritDoc} */
    @Override public boolean putValue(final ByteBuffer buf, int off, int len) throws IgniteCheckedException {
        assert valBytes != null : "Value bytes must be initialized before object is stored";

        return CacheObjectUtils.putValue(cacheObjectType(), buf, off, len, valBytes, 0);
    }

    /** {@inheritDoc} */
    @Override public int valueBytesLength(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valueBytes(ctx);

        return objectPutSize(valBytes.length);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(S.includeSensitive() ? getClass().getSimpleName() : "CacheObject",
            "val", val, true,
            "hasValBytes", valBytes != null, false);
    }
}
