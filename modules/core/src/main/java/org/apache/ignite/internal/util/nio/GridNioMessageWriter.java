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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Message writer.
 */
public interface GridNioMessageWriter {
    /**
     * @param nodeId Node ID.
     * @param msg Message to write.
     * @param buf Buffer.
     * @return Whether message was fully written.
     */
    public boolean write(@Nullable UUID nodeId, Message msg, ByteBuffer buf);

    /**
     * @param nodeId Node ID.
     * @param msg Message to write.
     * @param out Output stream to write to.
     * @param buf Buffer.
     * @return Number of bytes written.
     * @throws IOException In case of error.
     */
    public int writeFully(@Nullable UUID nodeId, Message msg, OutputStream out,
        ByteBuffer buf) throws IOException;
}