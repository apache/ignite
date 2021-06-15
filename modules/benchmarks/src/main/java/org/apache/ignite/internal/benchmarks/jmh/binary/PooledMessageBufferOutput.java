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

package org.apache.ignite.internal.benchmarks.jmh.binary;

import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferOutput;

import java.io.IOException;

/**
 * Fake pooled array output that gets rid of allocations in MsgPack benchmarks.
 */
public class PooledMessageBufferOutput implements MessageBufferOutput {
    private final byte[] data = new byte[128];

    private int pos;

    @Override
    public MessageBuffer next(int i) throws IOException {
        return MessageBuffer.wrap(data);
    }

    @Override
    public void writeBuffer(int position) throws IOException {
        // No-op.
        pos = position;
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        // No-op.
    }

    @Override
    public void add(byte[] bytes, int i, int i1) throws IOException {
        // No-op.
    }

    @Override
    public void close() throws IOException {
        // No-op.
    }

    @Override
    public void flush() throws IOException {
        // No-op.
    }

    public byte[] getData() {
        byte[] res = new byte[pos];

        System.arraycopy(data, 0, res, 0, pos);

        return res;
    }
}
