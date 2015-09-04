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

    /**
     * @param ctx Context.
     * @param cpy If {@code true} need to copy value.
     * @return Value.
     */
    @Nullable public <T> T value(CacheObjectContext ctx, boolean cpy);

    /**
     * @param ctx Context.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException;

    /**
     * @return Object type.
     */
    public byte type();

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
    public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException;
}