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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.function.ToIntFunction;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
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
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
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
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
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
     * @return Writer instance.
     */
    public BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema);

    /**
     * Create accessor for the field.
     *
     * @param field Field.
     * @param id FIeld ID.
     * @return Accessor.
     */
    public BinaryFieldAccessor create(Field field, int id);

    /**
     * @param ctor Constructor.
     * @param cls Class.
     * @return Instance.
     * @throws BinaryObjectException In case of error.
     */
    public Object newInstance(Constructor<?> ctor, Class<?> cls) throws BinaryObjectException;

    /**
     * Creates binary enum.
     *
     * @param ord Ordinal.
     * @param ctx Context.
     * @param typeId Type ID.
     */
    public BinaryObjectEx binaryEnum(BinaryContext ctx, int ord, @Nullable String clsName, int typeId);

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
     */
    public BinaryObjectEx binaryOffheapObject(BinaryContext ctx, long ptr, int start, int size);

    public BinaryObjectEx binaryObject()

    /**
     * @return Binary enum class.
     */
    public Class<?> binaryEnumClass();

    /**
     * @return Map of predefined types.
     */
    public Map<Class<?>, Integer> predefinedTypes();

    /**
     * @return Map of function returning size of the object.
     */
    public Map<Class<?>, ToIntFunction<Object>> sizeProviders();
}
