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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 *
 */
public class CacheObjectImpl implements CacheObject, Externalizable {
    /** */
    @GridToStringInclude
    @GridDirectTransient
    protected Object val;

    /** */
    protected byte[] valBytes;

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
    @Nullable @Override public <T> T getField(String name) {
        // TODO IGNITE-51.
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T value(GridCacheContext ctx) {
        if (val != null)
            return (T)val;

        assert valBytes != null;

        try {
            val = ctx.marshaller().unmarshal(valBytes, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unmarshal object.", e);
        }

        return (T)val;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        if (valBytes == null && !(val instanceof byte[]))
            valBytes = CU.marshal(ctx, val);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert val != null || valBytes != null;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        assert val != null || valBytes != null;

        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        boolean byteArr = val instanceof byte[];

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("valBytes", byteArr ? (byte[])val : valBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("byteArr", byteArr))
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

            case 1:
                boolean byteArr = reader.readBoolean("byteArr");

                if (!reader.isLastRead())
                    return false;

                if (byteArr) {
                    val = valBytes;

                    valBytes = null;
                }

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 89;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(GridCacheContext ctx) {
        return this;
    }

    /** {@inheritDoc} */
    public String toString() {
        if (val instanceof byte[])
            return getClass().getSimpleName() + " [val=<byte array>, len=" + ((byte[])val).length + ']';
        else
            return getClass().getSimpleName() + " [val=" + val + ", hasValBytes=" + (valBytes != null) + ']';
    }
}
