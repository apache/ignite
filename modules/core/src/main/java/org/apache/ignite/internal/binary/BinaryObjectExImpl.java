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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilders;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerInaccessibleClassException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.COLLECTION_LIMIT;

/**
 * Internal binary object interface.
 */
abstract class BinaryObjectExImpl implements BinaryObjectEx {
    /**
     * @return Length.
     */
    public abstract int length();

    /**
     * @return Object start.
     */
    public abstract int start();

    /**
     * @return {@code True} if object has bytes array.
     */
    public abstract boolean hasBytes();

    /**
     * @return Object array if object has byte array based, otherwise {@code null}.
     */
    public abstract byte[] bytes();

    /**
     * @return Object offheap address is object is offheap based, otherwise 0.
     */
    public abstract long offheapAddress();

    /**
     * Gets field value.
     *
     * @param fieldId Field ID.
     * @return Field value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of any other error.
     */
    @Nullable public abstract <F> F field(int fieldId) throws BinaryObjectException;

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /** {@inheritDoc} */
    @Override public String enumName() throws BinaryObjectException {
        throw new BinaryObjectException("Object is not enum.");
    }

    /**
     * Get offset of data begin.
     *
     * @return Field value.
     */
    public abstract int dataStartOffset();

    /**
     * Get offset of the footer begin.
     *
     * @return Field value.
     */
    public abstract int footerStartOffset();

    /**
     * Get field by offset.
     *
     * @param order Field offset.
     * @return Field value.
     */
    @Nullable public abstract <F> F fieldByOrder(int order);

    /**
     * Create field comparator.
     *
     * @return Comparator.
     */
    public abstract BinarySerializedFieldComparator createFieldComparator();

    /**
     * Writes field value defined by the given field offset to the given byte buffer.
     *
     * @param fieldOffset Field offset.
     * @return Boolean flag indicating whether the field was successfully written to the buffer, {@code false}
     *      if there is no enough space for the field in the buffer.
     */
    protected abstract boolean writeFieldByOrder(int fieldOffset, ByteBuffer buf);

    /**
     * @param ctx Reader context.
     * @param fieldName Field name.
     * @return Field value.
     */
    @Nullable protected abstract <F> F field(BinaryReaderHandles ctx, String fieldName);

    /**
     * @return {@code True} if object has schema.
     */
    public abstract boolean hasSchema();

    /**
     * Get schema ID.
     *
     * @return Schema ID.
     */
    public abstract int schemaId();

    /**
     * Create schema for object.
     *
     * @return Schema.
     */
    public abstract BinarySchema createSchema();

    /**
     * Get binary context.
     *
     * @return Binary context.
     */
    public abstract BinaryContext context();

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        return BinaryObjectBuilders.builder(this);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return (BinaryObject)super.clone();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (other == this)
            return true;

        if (!(other instanceof BinaryObject))
            return false;

        BinaryIdentityResolver identity = context().identity(typeId());

        return identity.equals(this, (BinaryObject)other);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        try {
            BinaryReaderHandles ctx = new BinaryReaderHandles();

            ctx.put(start(), this);

            return toString(ctx, new IdentityHashMap<BinaryObject, Integer>());
        }
        catch (BinaryObjectException e) {
            throw new IgniteException("Failed to create string representation of binary object.", e);
        }
    }

    /**
     * @param ctx Reader context.
     * @param handles Handles for already traversed objects.
     * @return String representation.
     */
    private String toString(BinaryReaderHandles ctx, IdentityHashMap<BinaryObject, Integer> handles) {
        int idHash = System.identityHashCode(this);
        int hash = hashCode();

        BinaryType meta;

        IgniteThread.onForbidBinaryMetadataRequestSectionEntered();

        try {
            meta = rawType();
        }
        catch (BinaryObjectException ignore) {
            meta = null;
        }
        finally {
            IgniteThread.onForbidBinaryMetadataRequestSectionLeft();
        }

        if (meta == null || !S.includeSensitive())
            return S.toString(S.includeSensitive() ? BinaryObject.class.getSimpleName() : "BinaryObject",
                "idHash", idHash, false,
                "hash", hash, false,
                "typeId", typeId(), true);

        handles.put(this, idHash);

        SB buf = new SB(meta.typeName());

        if (meta.fieldNames() != null) {
            buf.a(" [idHash=").a(idHash).a(", hash=").a(hash);

            for (String name : meta.fieldNames()) {
                Object val = fieldForToString(ctx, name);

                buf.a(", ").a(name).a('=');

                appendValue(val, buf, ctx, handles);
            }

            buf.a(']');
        }

        return buf.toString();
    }

    /** */
    private Object fieldForToString(BinaryReaderHandles ctx, String name) {
        try {
            return field(ctx, name);
        }
        catch (Exception e) {
            OptimizedMarshallerInaccessibleClassException e1 =
                X.cause(e, OptimizedMarshallerInaccessibleClassException.class);

            String msg = "Failed to create a string representation";

            return e1 != null
                ? "(" + msg + ": class not found " + e1.inaccessibleClass() + ")"
                : "(" + msg + ")";
        }
    }

    /**
     * @param val Value to append.
     * @param buf Buffer to append to.
     * @param ctx Reader context.
     * @param handles Handles for already traversed objects.
     */
    private void appendValue(Object val, SB buf, BinaryReaderHandles ctx,
        IdentityHashMap<BinaryObject, Integer> handles) {
        if (val instanceof byte[])
            buf.a(S.arrayToString(val));
        else if (val instanceof short[])
            buf.a(S.arrayToString(val));
        else if (val instanceof int[])
            buf.a(S.arrayToString(val));
        else if (val instanceof long[])
            buf.a(S.arrayToString(val));
        else if (val instanceof float[])
            buf.a(S.arrayToString(val));
        else if (val instanceof double[])
            buf.a(S.arrayToString(val));
        else if (val instanceof char[])
            buf.a(S.arrayToString(val));
        else if (val instanceof boolean[])
            buf.a(S.arrayToString(val));
        else if (val instanceof BigDecimal[])
            buf.a(S.arrayToString(val));
        else if (val instanceof IgniteUuid)
            buf.a(val);
        else if (val instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl po = (BinaryObjectExImpl)val;

            Integer idHash0 = handles.get(val);

            if (idHash0 != null) {  // Circular reference.
                BinaryType meta0 = po.rawType();

                assert meta0 != null;

                buf.a(meta0.typeName()).a(" [hash=").a(idHash0).a(", ...]");
            }
            else
                buf.a(po.toString(ctx, handles));
        }
        else if (val instanceof Object[]) {
            Object[] arr = (Object[])val;

            buf.a('[');

            int len = Math.min(arr.length, COLLECTION_LIMIT);

            for (int i = 0; i < len; i++) {
                Object o = arr[i];

                appendValue(o, buf, ctx, handles);

                if (i < len - 1)
                    buf.a(", ");
            }

            handleOverflow(buf, arr.length);

            buf.a(']');
        }
        else if (val instanceof Iterable) {
            Iterable<Object> col = (Iterable<Object>)val;

            buf.a(col.getClass().getSimpleName()).a(" {");

            Iterator it = col.iterator();

            int cnt = 0;

            while (it.hasNext()) {
                Object o = it.next();

                appendValue(o, buf, ctx, handles);

                if (++cnt == COLLECTION_LIMIT)
                    break;

                if (it.hasNext())
                    buf.a(", ");
            }

            if (it.hasNext())
                buf.a("... and more");

            buf.a('}');
        }
        else if (val instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>)val;

            buf.a(map.getClass().getSimpleName()).a(" {");

            Iterator<Map.Entry<Object, Object>> it = map.entrySet().iterator();

            int cnt = 0;

            while (it.hasNext()) {
                Map.Entry<Object, Object> e = it.next();

                appendValue(e.getKey(), buf, ctx, handles);

                buf.a('=');

                appendValue(e.getValue(), buf, ctx, handles);

                if (++cnt == COLLECTION_LIMIT)
                    break;

                if (it.hasNext())
                    buf.a(", ");
            }

            handleOverflow(buf, map.size());

            buf.a('}');
        }
        else
            buf.a(val);
    }

    /**
     * Writes overflow message to buffer if needed.
     *
     * @param buf String builder buffer.
     * @param size Size to compare with limit.
     */
    private static void handleOverflow(SB buf, int size) {
        int overflow = size - COLLECTION_LIMIT;

        if (overflow > 0)
            buf.a("... and ").a(overflow).a(" more");
    }

    /**
     * Check if object graph has circular references.
     *
     * @return {@code true} if object has circular references.
     */
    public boolean hasCircularReferences() {
        try {
            BinaryReaderHandles ctx = new BinaryReaderHandles();

            ctx.put(start(), this);

            return hasCircularReferences(ctx, new IdentityHashMap<BinaryObject, Integer>());
        }
        catch (BinaryObjectException e) {
            throw new IgniteException("Failed to check binary object for circular references", e);
        }
    }

    /**
     * @param ctx Reader context.
     * @param handles Handles for already traversed objects.
     * @return {@code true} if has circular reference.
     */
    private boolean hasCircularReferences(BinaryReaderHandles ctx, IdentityHashMap<BinaryObject, Integer> handles) {
        BinaryType meta;

        try {
            meta = rawType();
        }
        catch (BinaryObjectException ignore) {
            meta = null;
        }

        if (meta == null)
            return false;

        int idHash = System.identityHashCode(this);

        handles.put(this, idHash);

        if (meta.fieldNames() != null) {
            ctx.put(start(), this);

            for (String name : meta.fieldNames()) {
                Object val = field(ctx, name);

                if (val instanceof BinaryObjectExImpl) {
                    BinaryObjectExImpl po = (BinaryObjectExImpl)val;

                    Integer idHash0 = handles.get(val);

                    // Check for circular reference.
                    if (idHash0 != null || po.hasCircularReferences(ctx, handles))
                        return true;
                }
            }
        }

        return false;
    }
}
