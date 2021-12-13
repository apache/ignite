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

package org.apache.ignite.internal.processors.cache.persistence.wal.mmap;

import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.mmap.optane.OptaneUtil;

/**
 *  PMDK backed realization of {@link MmapProcessor }
 */
public class OptaneMmapProcessor extends MmapProcessor {
    /** {@inheritDoc}*/
    @Override public void start() throws IgniteCheckedException {
        super.start();

        log.info("Starting optane mmap processor;");
    }

    /**
     * @param ctx Kernal context.
     */
    public OptaneMmapProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc}*/
    @Override public ByteBufferHolder mmap(SegmentIO segmentIO, int szBytes) throws IOException {
        ByteBufferHolder holder = OptaneUtil.mmap(segmentIO, szBytes);

        if (log.isDebugEnabled()) {
            log.debug("Allocating byte buffer with pmdk [path=" + segmentIO.path() + ", isPmem="
                + (holder.type() == ByteBufferHolder.Type.OPTANE) + "]");
        }

        return holder;
    }

    /** {@inheritDoc}*/
    @Override public boolean isPersistentMemory(String path) {
        return OptaneUtil.isPersistentMemory(path);
    }
}
