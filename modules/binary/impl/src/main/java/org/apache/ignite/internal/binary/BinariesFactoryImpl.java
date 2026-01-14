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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Binary objects factory implementation.
 * @see CommonUtils#loadService(Class)
 */
public class BinariesFactoryImpl implements BinariesFactory {
    /** {@inheritDoc} */
    @Override public BinaryReaderEx reader(BinaryContext ctx, BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal) {
        return new BinaryReaderExImpl(ctx, in, ldr, forUnmarshal);
    }

    /** {@inheritDoc} */
    @Override public BinaryReaderEx reader(
        BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean forUnmarshal
    ) {
        return new BinaryReaderExImpl(ctx, in, ldr, hnds, forUnmarshal);
    }

    /** {@inheritDoc} */
    @Override public BinaryReaderEx reader(
        BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean skipHdrCheck,
        boolean forUnmarshal
    ) {
        return new BinaryReaderExImpl(ctx, in, ldr, hnds, skipHdrCheck, forUnmarshal);
    }

    /** {@inheritDoc} */
    @Override public BinaryWriterEx writer(BinaryContext ctx, boolean failIfUnregistered, int typeId) {
        BinaryThreadLocalContext locCtx = BinaryThreadLocalContext.get();

        return new BinaryWriterExImpl(
            ctx,
            BinaryStreams.outputStream((int)CommonUtils.KB, locCtx.chunk()),
            locCtx.schemaHolder(),
            null,
            failIfUnregistered,
            typeId
        );
    }

    /** {@inheritDoc} */
    @Override public BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out) {
        return new BinaryWriterExImpl(
            ctx,
            out,
            BinaryThreadLocalContext.get().schemaHolder(),
            null,
            false,
            GridBinaryMarshaller.UNREGISTERED_TYPE_ID
        );
    }

    /** {@inheritDoc} */
    @Override public BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema) {
        return new BinaryWriterExImpl(ctx, out, schema, null, false, GridBinaryMarshaller.UNREGISTERED_TYPE_ID);
    }

    /** {@inheritDoc} */
    @Override public BinaryFieldDescriptor create(Field field, int id) {
        BinaryWriteMode mode = BinaryUtils.mode(field.getType());

        switch (mode) {
            case P_BYTE:
            case P_BOOLEAN:
            case P_SHORT:
            case P_CHAR:
            case P_INT:
            case P_LONG:
            case P_FLOAT:
            case P_DOUBLE:
                return new BinaryFieldDescriptor(field, id, mode, GridUnsafe.objectFieldOffset(field), false);

            case BYTE:
            case BOOLEAN:
            case SHORT:
            case CHAR:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case STRING:
            case UUID:
            case DATE:
            case TIMESTAMP:
            case TIME:
            case BYTE_ARR:
            case SHORT_ARR:
            case INT_ARR:
            case LONG_ARR:
            case FLOAT_ARR:
            case DOUBLE_ARR:
            case CHAR_ARR:
            case BOOLEAN_ARR:
            case DECIMAL_ARR:
            case STRING_ARR:
            case UUID_ARR:
            case DATE_ARR:
            case TIMESTAMP_ARR:
            case TIME_ARR:
            case ENUM_ARR:
            case OBJECT_ARR:
            case BINARY_OBJ:
            case BINARY:
                return new BinaryFieldDescriptor(field, id, mode, -1L, false);

            default:
                return new BinaryFieldDescriptor(field, id, mode, -1L, !CommonUtils.isFinal(field.getType()));
        }
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectEx binaryEnum(BinaryContext ctx, int typeId, @Nullable String clsName, int ord) {
        return new BinaryEnumObjectImpl(ctx, typeId, clsName, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectEx binaryEnum(BinaryContext ctx, byte[] arr) {
        return new BinaryEnumObjectImpl(ctx, arr);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectEx binaryOffheapObject(BinaryContext ctx, long ptr, int start, int size) {
        return new BinaryObjectOffheapImpl(ctx, ptr, start, size);
    }

    /** {@inheritDoc} */
    @Override public Class<?> binaryEnumClass() {
        return BinaryEnumObjectImpl.class;
    }

    /** {@inheritDoc} */
    @Override public Class<?> binaryObjectImplClass() {
        return BinaryObjectImpl.class;
    }

    /** {@inheritDoc} */
    @Override public Map<Class<?>, Integer> predefinedTypes() {
        Map<Class<?>, Integer> predefinedTypes = new HashMap<>();

        predefinedTypes.put(BinaryEnumObjectImpl.class, 0);
        predefinedTypes.put(BinaryObjectOffheapImpl.class, 0);
        predefinedTypes.put(BinaryObjectImpl.class, 0);

        return predefinedTypes;
    }

    /** {@inheritDoc} */
    @Override public Map<Class<?>, ToIntFunction<Object>> sizeProviders() {
        return Map.of(
            BinaryObjectOffheapImpl.class, obj -> 0, // No extra heap memory.
            BinaryObjectImpl.class, new ToIntFunction<>() {
                private final long byteArrOffset = GridUnsafe.arrayBaseOffset(byte[].class);

                @Override public int applyAsInt(Object bo) {
                    return (int)GridUnsafe.align(byteArrOffset + ((BinaryObjectImpl)bo).bytes().length);
                }
            },
            BinaryEnumObjectImpl.class, bo -> ((BinaryObject)bo).size()
        );
    }

    /** {@inheritDoc} */
    @Override public int compareForDml(Object first, Object second) {
        return BinaryObjectImpl.compareForDml(first, second);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectEx binaryObject(BinaryContext ctx, byte[] arr, int start) {
        return newBinaryObject(ctx, arr, start);
    }

    /** */
    public static BinaryObjectEx newBinaryObject(BinaryContext ctx, byte[] arr, int start) {
        return new BinaryObjectImpl(ctx, arr, start);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectEx binaryObject(BinaryContext ctx, byte[] bytes) {
        return new BinaryObjectImpl(ctx, bytes);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObject(BinaryContext ctx, byte[] valBytes, CacheObjectValueContext coCtx) {
        return new BinaryObjectImpl(ctx, valBytes, coCtx);
    }

    /** {@inheritDoc} */
    @Override public boolean isBinaryObjectImpl(Object val) {
        return val instanceof BinaryObjectImpl;
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if {@code val} is assignable to binary Enum object.
     */
    public static boolean isAssignableToBinaryEnumObject(Class<?> cls) {
        return BinaryEnumObjectImpl.class.isAssignableFrom(cls);
    }
}
