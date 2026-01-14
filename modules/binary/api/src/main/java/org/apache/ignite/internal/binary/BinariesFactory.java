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
import java.util.Map;
import java.util.function.ToIntFunction;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for binaries classes creation.
 */
public interface BinariesFactory {
    /**
     * Creates reader instance.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param forUnmarshal {@code True} if reader is needed to unmarshal object.
     */
    public BinaryReaderEx reader(BinaryContext ctx, BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal);

    /**
     * Creates reader instance.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param forUnmarshal {@code True} if reader is needed to unmarshal object.
     */
    public BinaryReaderEx reader(BinaryContext ctx,
                                 BinaryInputStream in,
                                 ClassLoader ldr,
                                 @Nullable BinaryReaderHandles hnds,
                                 boolean forUnmarshal);

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param skipHdrCheck Whether to skip header check.
     * @param forUnmarshal {@code True} if reader is needed to unmarshal object.
     */
    public BinaryReaderEx reader(BinaryContext ctx,
                                 BinaryInputStream in,
                                 ClassLoader ldr,
                                 @Nullable BinaryReaderHandles hnds,
                                 boolean skipHdrCheck,
                                 boolean forUnmarshal);

    /**
     * @param ctx Context.
     * @param failIfUnregistered Flag to fail while writing object of unregistered type.
     * @param typeId Type id.
     * @return Writer instance.
     */
    public BinaryWriterEx writer(BinaryContext ctx, boolean failIfUnregistered, int typeId);

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @return Writer instance.
     */
    public BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out);

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param schema Schema holder
     * @return Writer instance.
     */
    public BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema);

    /**
     * Create accessor for the field.
     *
     * @param field Field.
     * @param id Field ID.
     * @return Accessor.
     */
    public BinaryFieldDescriptor create(Field field, int id);

    /**
     * Creates binary enum.
     *
     * @param typeId Type ID.
     * @param clsName Class name.
     * @param ord Ordinal.
     * @param ctx Context.
     */
    public BinaryObjectEx binaryEnum(BinaryContext ctx, int typeId, @Nullable String clsName, int ord);

    /**
     * Creates binary enum.
     *
     * @param ctx Context.
     * @param arr Array.
     */
    public BinaryObjectEx binaryEnum(BinaryContext ctx, byte[] arr);

    /**
     * @param ctx Context.
     * @param ptr Memory address.
     * @param start Object start.
     * @param size Memory size.
     * @return Binary object based on offheap memory.
     */
    public BinaryObjectEx binaryOffheapObject(BinaryContext ctx, long ptr, int start, int size);

    /**
     * @return Binary enum class.
     */
    public Class<?> binaryEnumClass();

    /**
     * @return Binary object impl class.
     */
    public Class<?> binaryObjectImplClass();

    /**
     * @return Map of predefined types.
     */
    public Map<Class<?>, Integer> predefinedTypes();

    /**
     * @return Map of function returning size of the object.
     */
    public Map<Class<?>, ToIntFunction<Object>> sizeProviders();

    /**
     * Compare two objects for DML operation.
     *
     * @param first First.
     * @param second Second.
     * @return Comparison result which is aligned with the {@link java.util.Comparator#compare(Object, Object)} contract.
     */
    public int compareForDml(Object first, Object second);

    /** Creates new instance of binary object. */
    public BinaryObjectEx binaryObject(BinaryContext ctx, byte[] arr, int start);

    /** Creates new instance of binary object. */
    public BinaryObjectEx binaryObject(BinaryContext ctx, byte[] bytes);

    /** Creates new instance of binary object. */
    public BinaryObject binaryObject(BinaryContext ctx, byte[] valBytes, CacheObjectValueContext coCtx);

    /** */
    public BinaryIdentityResolver arrayIdentityResolver();

    /**
     * Clears binary caches.
     */
    public void clearCache();
}
