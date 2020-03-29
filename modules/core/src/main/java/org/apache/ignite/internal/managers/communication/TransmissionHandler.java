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

package org.apache.ignite.internal.managers.communication;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Class represents a handler for the set of files considered to be transferred from the remote node. This handler
 * must be registered to and appropriate topic in {@link GridIoManager} prior to opening a new transmission connection
 * to this topic.
 * <p>
 * <em>NOTE:</em> Only one such handler per registered topic is allowed for the communication
 * manager. Only one thread is allowed for data processing within a single topic.
 *
 * <h3>TransmissionPolicy</h3>
 * <p>
 * Files from the remote node can be handled of two different ways within a single established connection.
 * It is up to the sender to decide how the particular file must be processed by the remote node. The
 * {@link TransmissionPolicy} is used for such purpose. If {@link TransmissionPolicy#FILE} type is received by
 * remote node the <em>#fileHandler()</em> will be picked up to process this file, the otherwise for the
 * {@link TransmissionPolicy#CHUNK} the <em>#chunkHandler()</em> will be picked up.
 */
public interface TransmissionHandler {
    /**
     * <em>Chunk handler</em> represents by itself the way of input data stream processing.
     * It accepts within each chunk a {@link ByteBuffer} with data from input for further processing.
     * Activated when the {@link TransmissionPolicy#CHUNK} policy sent.
     * <p>
     * The {@link TransmissionCancelledException} can be thrown to gracefully interrupt the local transmission and
     * the node-senders transmission session.
     *
     * @param nodeId Remote node id from which request has been received.
     * @param initMeta Initial handler meta info.
     * @return Instance of chunk handler to process incoming data by chunks.
     */
    public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta);

    /**
     * Absolute path of a file to receive remote transmission data into. The {@link TransmissionCancelledException}
     * can be thrown if it is necessary to gracefully interrupt current transmission session on the node-sender.
     *
     * @param nodeId Remote node id from which request has been received.
     * @param fileMeta File meta info.
     * @return Absolute pathname denoting a file.
     */
    public String filePath(UUID nodeId, TransmissionMeta fileMeta);

    /**
     * <em>File handler</em> represents by itself the way of input data stream processing. All the data will
     * be processed under the hood using zero-copy transferring algorithm and only start file processing and
     * the end of processing will be provided. Activated when the {@link TransmissionPolicy#FILE} policy sent.
     * <p>
     * The {@link TransmissionCancelledException} can be thrown to gracefully interrupt the local transmission and
     * the node-senders transmission session.
     *
     * @param nodeId Remote node id from which request has been received.
     * @param initMeta Initial handler meta info.
     * @return Intance of read handler to process incoming data like the {@link FileChannel} manner.
     */
    public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta);

    /**
     * @param nodeId Remote node id on which the error occurred.
     * @param err The err of fail handling process.
     */
    public void onException(UUID nodeId, Throwable err);

    /**
     * The end of the handled transmission. This means that all resources associated with previously opened
     * session is freed and can be reused. Generally, it means that transmission topic from now can
     * safely accept new files.
     *
     * @param rmtNodeId Remote node id from which the source request comes from.
     */
    public void onEnd(UUID rmtNodeId);
}
