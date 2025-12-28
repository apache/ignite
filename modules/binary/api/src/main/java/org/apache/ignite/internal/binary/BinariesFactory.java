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

import org.apache.ignite.internal.binary.streams.BinaryInputStream;
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
}
