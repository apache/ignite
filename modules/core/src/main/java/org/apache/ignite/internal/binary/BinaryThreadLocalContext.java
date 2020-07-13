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

import org.apache.ignite.internal.binary.streams.BinaryMemoryAllocator;
import org.apache.ignite.internal.binary.streams.BinaryMemoryAllocatorChunk;

/**
 * Contains thread-local data for binary marshalling.
 */
public class BinaryThreadLocalContext {
    /** Thread-local instance. */
    private static final ThreadLocal<BinaryThreadLocalContext> CTX = new ThreadLocal<BinaryThreadLocalContext>() {
        @Override protected BinaryThreadLocalContext initialValue() {
            return new BinaryThreadLocalContext();
        }
    };

    /** Memory chunk. */
    private final BinaryMemoryAllocatorChunk chunk = BinaryMemoryAllocator.INSTANCE.chunk();

    /** Schema holder. */
    private final BinaryWriterSchemaHolder schema = new BinaryWriterSchemaHolder();

    /**
     * Get current context.
     *
     * @return Context.
     */
    public static BinaryThreadLocalContext get() {
        return CTX.get();
    }

    /**
     * Private constructor.
     */
    private BinaryThreadLocalContext() {
        // No-op.
    }

    /**
     * @return Memory chunk.
     */
    public BinaryMemoryAllocatorChunk chunk() {
        return chunk;
    }

    /**
     * @return Schema holder.
     */
    public BinaryWriterSchemaHolder schemaHolder() {
        return schema;
    }
}
