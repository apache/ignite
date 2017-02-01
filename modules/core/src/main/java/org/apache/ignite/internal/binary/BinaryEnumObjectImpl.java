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

package org.apache.ignite.internal.binary;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Binary enum object.
 */
public class BinaryEnumObjectImpl implements BinaryObjectEx, Externalizable, CacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    @GridDirectTransient
    private BinaryContext ctx;

    /** Type ID. */
    private int typeId;

    /** Raw data. */
    private String clsName;

    /** Ordinal. */
    private int ord;

    /**
     * {@link Externalizable} support.
     */
    public BinaryEnumObjectImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param typeId Type ID.
     * @param clsName Class name.
     * @param ord Ordinal.
     */
    public BinaryEnumObjectImpl(BinaryContext ctx, int typeId, @Nullable String clsName, int ord) {
        assert ctx != null;

        this.ctx = ctx;
        this.typeId = typeId;
        this.clsName = clsName;
        this.ord = ord;
    }

    /**
     * @param ctx Context.
     * @param arr Array.
     */
    public BinaryEnumObjectImpl(BinaryContext ctx, byte[] arr) {
        assert ctx != null;
        assert arr != null;
        assert arr[0] == GridBinaryMarshaller.ENUM;

        this.ctx = ctx;

        int off = 1;

        this.typeId = BinaryPrimitives.readInt(arr, off);

        off += 4;

        if (this.typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            assert arr[off] == GridBinaryMarshaller.STRING;

            int len = BinaryPrimitives.readInt(arr, ++off);

            off += 4;

            byte[] bytes = BinaryPrimitives.readByteArray(arr, off, len);

            off += len;

            this.clsName = new String(bytes, UTF_8);
        }

        this.ord = BinaryPrimitives.readInt(arr, off);
    }

    /**
     * @return Class name.
     */
    @Nullable public String className() {
        return clsName;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public BinaryType type() throws BinaryObjectException {
        return BinaryUtils.typeProxy(ctx, this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType rawType() throws BinaryObjectException {
        return BinaryUtils.type(ctx, this);
    }

    /** {@inheritDoc} */
    @Override public boolean isFlagSet(short flag) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T deserialize() throws BinaryObjectException {
        Class cls = BinaryUtils.resolveClass(ctx, typeId, clsName, ctx.configuration().getClassLoader(), true);

        return (T)BinaryEnumCache.get(cls, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return (BinaryObject)super.clone();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        throw new UnsupportedOperationException("Builder cannot be created for enum.");
    }

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        return ord;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * typeId + ord;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj != null && (obj instanceof BinaryEnumObjectImpl)) {
            BinaryEnumObjectImpl other = (BinaryEnumObjectImpl)obj;

            return typeId == other.typeId && ord == other.ord;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (!S.INCLUDE_SENSITIVE)
            return ord >= 0 ? "BinaryEnum" : "null";

        // 1. Try deserializing the object.
        try {
            Object val = deserialize();

            return new SB().a(val).toString();
        }
        catch (Exception ignored) {
            // No-op.
        }

        // 2. Try getting meta.
        BinaryType type;

        try {
            type = rawType();
        }
        catch (Exception ignored) {
            type = null;
        }

        if (type != null)
            return S.toString(type.typeName(), "ordinal", ord, true);
        else {
            if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
                return S.toString("BinaryEnum", "clsName", clsName, true, "ordinal", ord, true);
            else
                return S.toString("BinaryEnum", "typeId", typeId, true, "ordinal", ord, true);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(typeId);
        out.writeObject(clsName);
        out.writeInt(ord);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = GridBinaryMarshaller.threadLocalContext();

        typeId = in.readInt();
        clsName = (String)in.readObject();
        ord = in.readInt();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
        return deserialize();
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectContext cacheCtx) throws IgniteCheckedException {
        return U.marshal(ctx.marshaller(), this);
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        return TYPE_BINARY_ENUM;
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        this.ctx = ((CacheObjectBinaryProcessorImpl)ctx.processor()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 119;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
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
                if (!writer.writeString("clsName", clsName))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("ord", ord))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("typeId", typeId))
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
                clsName = reader.readString("clsName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ord = reader.readInt("ord");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                typeId = reader.readInt("typeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(BinaryEnumObjectImpl.class);
    }

    /**
     * @param cls type to examine.
     * @return true if typeId equals for passed type and current
     * binary enum.
     */
    public boolean isTypeEquals(final Class<?> cls) {
        return ctx.descriptorForClass(cls, false).typeId() == typeId();
    }
}
