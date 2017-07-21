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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface CacheObject extends Message {
    /** */
    public static final byte TYPE_REGULAR = 1;

    /** */
    public static final byte TYPE_BYTE_ARR = 2;

    /** */
    public static final byte TYPE_BINARY = 100;

    /** */
    public static final byte TYPE_BINARY_ENUM = 101;

    /**
     * @param ctx Context.
     * @param cpy If {@code true} need to copy value.
     * @return Value.
     */
    @Nullable public <T> T value(CacheObjectValueContext ctx, boolean cpy);

    /**
     * @param ctx Context.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException;

    /**
     * @param ctx Cache object context.
     * @return Size required to store this value object.
     * @throws IgniteCheckedException If failed.
     */
    public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException;

    /**
     * @param buf Buffer to write value to.
     * @return {@code True} if value was successfully written, {@code false} if there was not enough space in the
     *      buffer.
     * @throws IgniteCheckedException If failed.
     */
    public boolean putValue(ByteBuffer buf) throws IgniteCheckedException;

    /**
     * @param addr Address tp write value to.
     * @return Number of bytes written.
     * @throws IgniteCheckedException If failed.
     */
    public int putValue(long addr) throws IgniteCheckedException;

    /**
     * @param buf Buffer to write value to.
     * @param off Offset in source binary data.
     * @param len Length of the data to write.
     * @return {@code True} if value was successfully written, {@code false} if there was not enough space in the
     *      buffer.
     * @throws IgniteCheckedException If failed.
     */
    public boolean putValue(ByteBuffer buf, int off, int len) throws IgniteCheckedException;

    /**
     * @return Object type.
     */
    public byte cacheObjectType();

    /**
     * Gets flag indicating whether object value is a platform type. Platform types will be automatically
     * deserialized on public API cache operations regardless whether
     * {@link org.apache.ignite.IgniteCache#withKeepBinary()} is used or not.
     *
     * @return Platform type flag.
     */
    public boolean isPlatformType();

    /**
     * Prepares cache object for cache (e.g. copies user-provided object if needed).
     *
     * @param ctx Cache context.
     * @return Instance to store in cache.
     */
    public CacheObject prepareForCache(CacheObjectContext ctx);

    /**
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException;
}