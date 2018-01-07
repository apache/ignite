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

package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface that contains methods to compress and decompress data.
 */
public interface CompressionEngine {
    /**
     * Compress source byte buffer.
     * @param src ByteBuffer containing data to compress.
     * @param buf ByteBuffer to hold outbound network data.
     * @return Result of compression.
     */
    CompressionEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException;

    /**
     * Decompress source byte buffer that can contains part of message, one or more messages.
     * @param src ByteBuffer containing compressed data.
     * @param buf ByteBuffer to hold inbound network data.
     * @return Result of decompression.
     */
    CompressionEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException;
}
