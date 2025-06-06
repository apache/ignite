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
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public abstract class CacheObjectAdapter implements CacheObject, Externalizable {
    /** */
    private static final long serialVersionUID = 2006765505127197251L;

    /** Head size. */
    protected static final int HEAD_SIZE = 5; // 4 bytes len + 1 byte type

    /** */
    @GridToStringInclude(sensitive = true)
    @GridDirectTransient
    protected Object val;

    /** */
    protected byte[] valBytes;

    /**
     * @param ctx Context.
     * @return {@code True} need to copy value returned to user.
     */
    protected boolean needCopy(CacheObjectValueContext ctx) {
        return ctx.copyOnGet() && val != null && !ctx.kernalContext().cacheObjects().immutable(val);
    }

    /**
     * @return Value bytes from value.
     */
    protected byte[] valueBytesFromValue(CacheObjectValueContext ctx) throws IgniteCheckedException {
        byte[] bytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);

        return CacheObjectTransformerUtils.transformIfNecessary(bytes, ctx);
    }

    /**
     * @return Value from value bytes.
     */
    protected Object valueFromValueBytes(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        byte[] bytes = CacheObjectTransformerUtils.restoreIfNecessary(valBytes, ctx);

        return ctx.kernalContext().cacheObjects().unmarshal(ctx, bytes, ldr);
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

        return putValue(addr, cacheObjectType(), valBytes);
    }

    /**
     * @param addr Write address.
     * @param type Object type.
     * @param valBytes Value bytes array.
     * @return Offset shift compared to initial address.
     */
    public static int putValue(long addr, byte type, byte[] valBytes) {
        return putValue(addr, type, valBytes, 0, valBytes.length);
    }

    /**
     * @param addr Write address.
     * @param type Object type.
     * @param srcBytes Source value bytes array.
     * @param srcOff Start position in sourceBytes.
     * @param len Number of bytes for write.
     * @return Offset shift compared to initial address.
     */
    public static int putValue(long addr, byte type, byte[] srcBytes, int srcOff, int len) {
        int off = 0;

        PageUtils.putInt(addr, off, len);
        off += 4;

        PageUtils.putByte(addr, off, type);
        off++;

        PageUtils.putBytes(addr, off, srcBytes, srcOff, len);
        off += len;

        return off;
    }

    /** {@inheritDoc} */
    @Override public boolean putValue(final ByteBuffer buf, int off, int len) throws IgniteCheckedException {
        assert valBytes != null : "Value bytes must be initialized before object is stored";

        return putValue(cacheObjectType(), buf, off, len, valBytes, 0);
    }

    /** {@inheritDoc} */
    @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valueBytes(ctx);

        return objectPutSize(valBytes.length);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                valBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray(valBytes))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(S.includeSensitive() ? getClass().getSimpleName() : "CacheObject",
            "val", val, true,
            "hasValBytes", valBytes != null, false);
    }

    /**
     * @param dataLen Serialized value length.
     * @return Full size required to store cache object.
     * @see #putValue(byte, ByteBuffer, int, int, byte[], int)
     */
    public static int objectPutSize(int dataLen) {
        return dataLen + HEAD_SIZE;
    }

    /**
     * @param cacheObjType Cache object type.
     * @param buf Buffer to write value to.
     * @param off Offset in source binary data.
     * @param len Length of the data to write.
     * @param valBytes Binary data.
     * @param start Start offset in binary data.
     * @return {@code True} if data were successfully written.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean putValue(byte cacheObjType,
        final ByteBuffer buf,
        int off,
        int len,
        byte[] valBytes,
        final int start
    ) throws IgniteCheckedException {
        int dataLen = valBytes.length;

        if (buf.remaining() < len)
            return false;

        if (off == 0 && len >= HEAD_SIZE) {
            buf.putInt(dataLen);
            buf.put(cacheObjType);

            len -= HEAD_SIZE;
        }
        else if (off >= HEAD_SIZE)
            off -= HEAD_SIZE;
        else {
            // Partial header write.
            final ByteBuffer head = ByteBuffer.allocate(HEAD_SIZE);

            head.order(buf.order());

            head.putInt(dataLen);
            head.put(cacheObjType);

            head.position(off);

            if (len < head.capacity())
                head.limit(off + Math.min(len, head.capacity() - off));

            buf.put(head);

            if (head.limit() < HEAD_SIZE)
                return true;

            len -= HEAD_SIZE - off;
            off = 0;
        }

        buf.put(valBytes, start + off, len);

        return true;
    }
}
