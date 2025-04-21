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

import org.apache.ignite.IgniteSystemProperties;

/**
 * Memory allocator chunk.
 */
public interface BinaryMemoryAllocatorChunk {
    /** @see IgniteSystemProperties#IGNITE_MARSHAL_BUFFERS_RECHECK */
    public static final int DFLT_MARSHAL_BUFFERS_RECHECK = 10000;

    /** @see IgniteSystemProperties#IGNITE_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE */
    public static final int DFLT_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE = 32;


    /**
     * Allocate.
     *
     * @param size Desired size.
     * @return Data.
     */
    public byte[] allocate(int size);

    /**
     * Reallocate.
     *
     * @param data Old data.
     * @param size Size.
     * @return New data.
     */
    public byte[] reallocate(byte[] data, int size);

    /**
     * Shrinks array size if needed.
     */
    public void release(byte[] data, int maxMsgSize);

    /**
     * @return {@code True} if acquired.
     */
    public boolean isAcquired();
}
