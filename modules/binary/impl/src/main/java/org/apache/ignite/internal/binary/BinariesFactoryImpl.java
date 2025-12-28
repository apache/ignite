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
import org.apache.ignite.internal.util.CommonUtils;
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
}
