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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 *
 */
public class KeyCacheObjectImpl extends CacheObjectAdapter implements KeyCacheObject, Comparable<KeyCacheObjectImpl> {
    /** */
    static final int DIRECT_TYPE = 90;

    /**
     *
     */
    public KeyCacheObjectImpl() {
        // No-op.
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public KeyCacheObjectImpl(Object val, byte[] valBytes) {
        assert val != null;
        assert valBytes != null || this instanceof UserKeyCacheObjectImpl : this;

        this.val = val;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int compareTo(KeyCacheObjectImpl other) {
        assert val instanceof Comparable : val;
        assert other.val instanceof Comparable : val;

        return ((Comparable)val).compareTo(other.val);
    }

    /** {@inheritDoc} */
    @Override public boolean byteArray() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(GridCacheContext ctx) throws IgniteCheckedException {
        assert valBytes != null : this;

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        assert val != null;

        return val instanceof GridCacheInternal;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T value(GridCacheContext ctx, boolean cpy) {
        cpy = cpy && needCopy(ctx);

        if (cpy) {
            try {
                return (T)ctx.portable().unmarshal(ctx.cacheObjectContext(),
                    valBytes,
                    ctx.deploy().globalLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal object.", e);
            }
        }

        return (T)val;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(GridCacheContext ctx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert val != null;

        return val.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("valBytes", valBytes))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                valBytes = reader.readByteArray("valBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return DIRECT_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = CU.marshal(ctx.kernalContext().cache().context(), val);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (val == null) {
            assert valBytes != null;

            val = ctx.marshaller().unmarshal(valBytes, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof KeyCacheObjectImpl))
            return false;

        KeyCacheObjectImpl other = (KeyCacheObjectImpl)obj;

        return val.equals(other.val);
    }
}
