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

package org.apache.ignite.internal.binary.streams;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;
import org.apache.ignite.internal.util.CommonUtils;

/**
 * Binary streams factory.
 * Implementation loaded via {@link ServiceLoader} mechanism.
 *
 * @see CommonUtils#loadService(Class)
 * @see BinaryStreams
 */
public interface BinaryStreamsFactory {
    /**
     * Create stream with pointer set at the given position.
     *
     * @param data Data.
     * @param pos Position.
     * @return Stream.
     */
    public BinaryInputStream inputStream(byte[] data, int pos);

    /**
     * Create stream with pointer set at the given position.
     *
     * @param data Data.
     * @return Stream.
     */
    public BinaryInputStream inputStream(byte[] data);

    /**
     * @param buf Buffer to wrap.
     * @return Stream.
     */
    public BinaryInputStream inputStream(ByteBuffer buf);

    /**
     * @param ptr Pointer.
     * @param cap Capacity.
     * @return Stream.
     */
    public BinaryInputStream inputStream(long ptr, int cap);

    /**
     * @param ptr Pointer.
     * @param cap Capacity.
     * @param forceHeap If {@code true} method {@link BinaryInputStream#offheapPointer()} returns 0 and unmarshalling will
     *        create heap-based objects.
     * @return Stream.
     */
    public BinaryInputStream inputStream(long ptr, int cap, boolean forceHeap);

    /**
     * @param cap Capacity.
     * @param disableAutoClose Whether to disable resource release in {@link BinaryOutputStream#close()} method
     *                         so that an explicit {@link BinaryOutputStream#release()} call is required.
     * @return Binary output stream data.
     */
    public BinaryOutputStream createPooledOutputStream(int cap, boolean disableAutoClose);

    /**
     * @param cap Capacity.
     * @return Binary output stream data.
     */
    public BinaryOutputStream outputStream(int cap);

    /**
     * @param cap Capacity.
     * @param chunk Memory allocator chunk.
     * @return Binary output stream.
     */
    public BinaryOutputStream outputStream(int cap, BinaryMemoryAllocatorChunk chunk);

    /**
     * @return Thread local binary memory allocator.
     */
    public BinaryMemoryAllocatorChunk threadLocalChunk();
}
