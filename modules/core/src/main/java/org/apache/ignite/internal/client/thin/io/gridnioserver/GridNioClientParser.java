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

package org.apache.ignite.internal.client.thin.io.gridnioserver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.ignite.internal.client.thin.io.ClientMessageDecoder;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

public class GridNioClientParser implements GridNioParser {
    /** */
    private final ClientMessageDecoder decoder = new ClientMessageDecoder();

    /** {@inheritDoc} */
    @Override public @Nullable Object decode(GridNioSession ses, ByteBuffer buf) {
        byte[] bytes = decoder.apply(buf);

        if (bytes == null)
            return null; // Message is not yet completely received.

        return ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) {
        return (ByteBuffer)msg;
    }
}
