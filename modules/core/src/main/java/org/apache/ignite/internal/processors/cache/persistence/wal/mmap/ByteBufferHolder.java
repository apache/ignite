/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.mmap;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;

/**
 * Base interface of ByteBuffer holder.
 */
public interface ByteBufferHolder extends AutoCloseable {
    /**
     * @return Holded buffer.
     */
    ByteBuffer buffer();

    /**
     * @return Capacity of holded buffer.
     */
    int capacity();

    /**
     * @return Free holded buffer.
     */
    void free();

    /**
     * Perform memory sync of holded buffer.
     *
     * @param off Offset within buffer.
     * @param len Length of data to sync.
     * @throws IgniteCheckedException If failed.
     */
    void msync(int off, int len) throws IgniteCheckedException;

    /**
     * Perform memory sync of holded buffer.
     *
     * @throws IgniteCheckedException If failed.
     */
    void msync() throws IgniteCheckedException;

    /**
     * @return Type of holdel buffer.
     */
    Type type();

    /**
     * Type of buffers.
     */
    public enum Type {
        /** On heap buffer. */
        ONHEAP,
        /** Off heap direct buffer. */
        DIRECT,
        /** Memory mapped direct buffer */
        MMAP,
        /** Persistent memory mapped direct buffer */
        OPTANE
    }
}
