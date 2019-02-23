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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.FileMetaInfo;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/** */
public class FileTransferManager<T extends FileMetaInfo> implements AutoCloseable {
    /** */
    private static final String ACK_MSG = "d2738352-8813-477a-a165-b73249798134";

    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

    /** */
    protected final SocketChannel channel;

    /** */
    protected final FileIOFactory factory;

    /** */
    protected final IgniteLogger log;

    /** */
    protected final DataInputStream dis;

    /** */
    protected final DataOutputStream dos;

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @param factory Factory to provide io over files.
     * @throws IOException If fails.
     */
    public FileTransferManager(
        GridKernalContext ktx,
        SocketChannel channel,
        FileIOFactory factory
    ) throws IOException {
        this.channel = channel;
        this.dis = new DataInputStream(channel.socket().getInputStream());
        this.dos = new DataOutputStream(channel.socket().getOutputStream());
        this.factory = factory;
        this.log = ktx.log(getClass());
    }

    /**
     * @param meta File meta info to read from.
     * @throws IgniteCheckedException If fails.
     */
    public void readFileMetaInfo(T meta) throws IgniteCheckedException {
        try {
            meta.readMetaInfo(dis);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param meta File meta info to write at.
     * @throws IgniteCheckedException If fails.
     */
    public void writeFileMetaInfo(T meta) throws IgniteCheckedException {
        try {
            meta.writeMetaInfo(dos);

            dos.flush();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @throws IOException If fails.
     */
    private void readAck() throws IOException {
        if (!ACK_MSG.equals(dis.readUTF()))
            throw new IOException("Incorrect ack message");
    }

    /**
     * @throws IOException If fails.
     */
    private void writeAck() throws IOException {
        dos.writeUTF(ACK_MSG);

        dos.flush();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        dos.close();
        dis.close();
        channel.close();
    }

    /**
     * @param file File to download into.
     * @param count The number of bytes to expect.
     * @throws IgniteCheckedException If fails.
     */
    public void readFile(File file, long count) throws IgniteCheckedException {
        try {
            try (FileIO fileIO = factory.create(file, CREATE, WRITE)) {
                while (count > 0) {
                    long written = fileIO.transferFrom(channel, fileIO.position(), count);

                    if (written < 0)
                        break;

                    count -= written;
                }
            }

            writeAck();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param file Partition file.
     * @throws IgniteCheckedException If fails.
     */
    public void writeFile(File file, long offset, long size) throws IgniteCheckedException {
        FileIO fileIO = null;

        try {
            fileIO = factory.create(file, READ);

            // Send the whole file to channel.
            // Todo limit thransfer speed
            long written = 0;

            while (written < size)
                written += fileIO.transferTo(written, CHUNK_SIZE, channel);

            //Waiting for the writing response.
            readAck();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            U.closeQuiet(fileIO);
        }
    }
}
