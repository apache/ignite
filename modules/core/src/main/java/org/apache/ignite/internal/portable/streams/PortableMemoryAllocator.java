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

package org.apache.ignite.internal.portable.streams;

/**
 * Portable memory allocator.
 */
public interface PortableMemoryAllocator {
    /** Default memory allocator. */
    public static final PortableMemoryAllocator DFLT_ALLOC = new PortableSimpleMemoryAllocator();

    /**
     * Allocate memory.
     *
     * @param size Size.
     * @return Data.
     */
    public byte[] allocate(int size);

    /**
     * Reallocates memory.
     *
     * @param data Current data chunk.
     * @param size New size required.
     *
     * @return Data.
     */
    public byte[] reallocate(byte[] data, int size);

    /**
     * Release memory.
     *
     * @param data Data.
     * @param maxMsgSize Max message size sent during the time the allocator is used.
     */
    public void release(byte[] data, int maxMsgSize);

    /**
     * Allocate memory.
     *
     * @param size Size.
     * @return Address.
     */
    public long allocateDirect(int size);

    /**
     * Reallocate memory.
     *
     * @param addr Address.
     * @param size Size.
     * @return Address.
     */
    public long reallocateDirect(long addr, int size);

    /**
     * Release memory.
     *
     * @param addr Address.
     */
    public void releaseDirect(long addr);
}