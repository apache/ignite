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

import java.io.IOException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;

/**
 * Processor helps to generate memory mapped file buffers.
 */
public class MmapProcessor extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public MmapProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Memory maps wal segment to memory.
     *
     * @param segmentIO Wal segment.
     * @param szBytes Size of buffer.
     * @return Instance of {@link ByteBufferHolder}.
     * @throws IOException If failed.
     */
    public ByteBufferHolder mmap(SegmentIO segmentIO, int szBytes) throws IOException {
        return new MappedByteBufferHolder(segmentIO.map(szBytes));
    }

    /**
     * @return {@code true} if path are on persistent memory.
     */
    public boolean isPersistentMemory(String path) {
        return false;
    }
}
