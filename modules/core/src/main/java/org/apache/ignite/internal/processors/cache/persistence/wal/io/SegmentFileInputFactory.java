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
