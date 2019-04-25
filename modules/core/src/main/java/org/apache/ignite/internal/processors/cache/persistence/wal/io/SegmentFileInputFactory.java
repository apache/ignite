/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.io;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;

/**
 * Factory to provide I/O interfaces for read primitives with files.
 */
public interface SegmentFileInputFactory {
    /**
     * @param segmentIO FileIO of segment for reading.
     * @param buf ByteBuffer wrapper for dynamically expand buffer size.
     * @return Instance of {@link FileInput}.
     * @throws IOException If have some trouble with I/O.
     */
    FileInput createFileInput(SegmentIO segmentIO, ByteBufferExpander buf) throws IOException;
}
