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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Byte buffer backed data input.
 */
public class ByteBufferBackedDataInputImpl extends ByteBufferBackedDataInput {
    /** Buffer. */
    private ByteBuffer buf;

    /**
     * @param buf New buffer.
     */
    public ByteBufferBackedDataInput buffer(ByteBuffer buf) {
        this.buf = buf;

        return this;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer buffer() {
        return buf;
    }

    /** {@inheritDoc} */
    @Override public void ensure(int requested) throws IOException {
        if (buffer().remaining() < requested)
            throw new IOException("Requested size is greater than buffer: " + requested);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return buffer().limit();
    }
}
