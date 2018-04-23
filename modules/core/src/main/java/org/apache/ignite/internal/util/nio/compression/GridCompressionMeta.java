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

package org.apache.ignite.internal.util.nio.compression;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;

/**
 * Compression meta.
 */
public final class GridCompressionMeta {
    /** GridNioCompressionHandler. */
    private GridNioCompressionHandler hnd;

    /** Data already decoded by blocking Compress handler. */
    private ByteBuffer decodedBuf;

    /** Data read but not decoded by blocking Compress handler. */
    private ByteBuffer encodedBuf;

    /** Compression engine. */
    private CompressionEngine compressionEngine;

    /** */
    public CompressionEngine compressionEngine() {
        return compressionEngine;
    }

    /**
     * @param compressionEngine Compress engine.
     */
    public void compressionEngine(CompressionEngine compressionEngine) {
        assert compressionEngine != null;

        this.compressionEngine = compressionEngine;
    }

    /** */
    @Nullable public GridNioCompressionHandler handler() {
        return hnd;
    }

    /**
     * @param hnd Handler.
     */
    public void handler(GridNioCompressionHandler hnd) {
        assert hnd != null;

        this.hnd = hnd;
    }

    /** */
    @Nullable ByteBuffer decodedBuffer() {
        return decodedBuf;
    }

    /**
     * @param decodedBuf Decoded buffer.
     */
    public void decodedBuffer(ByteBuffer decodedBuf) {
        assert decodedBuf != null;

        this.decodedBuf = decodedBuf;
    }

    /** */
    @Nullable ByteBuffer encodedBuffer() {
        return encodedBuf;
    }

    /**
     * @param encodedBuf Encoded buffer.
     */
    public void encodedBuffer(ByteBuffer encodedBuf) {
        assert encodedBuf != null;

        this.encodedBuf = encodedBuf;
    }
}
