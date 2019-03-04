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
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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

    /** The default factory to provide IO oprations over downloading files. */
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** */
    protected final SocketChannel channel;

    /** */
    protected final IgniteLogger log;

    /** */
    protected final DataInputStream dis;

    /** */
    protected final DataOutputStream dos;

    /**
     * @param ktx Kernal context.
     * @param channel Socket channel to upload files to.
     * @throws IOException If fails.
     */
    public FileTransferManager(
        GridKernalContext ktx,
        SocketChannel channel
    ) throws IOException {
        assert channel.isBlocking();

        this.channel = channel;
        this.dis = new DataInputStream(channel.socket().getInputStream());
        this.dos = new DataOutputStream(channel.socket().getOutputStream());
        this.log = ktx.log(getClass());
    }

    /**
     * @param meta File meta info to read from.
     * @throws IgniteCheckedException If fails.
     */
    public void readMetaInto(T meta) throws IgniteCheckedException {
        try {
            meta.readMetaInfo(dis);
        }
        catch (EOFException e) {
            throw new IgniteCheckedException("Input connection closed unexpectedly", e);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param meta File meta info to write at.
     * @throws IgniteCheckedException If fails.
     */
    public void writeMetaFrom(T meta) throws IgniteCheckedException {
        try {
            meta.writeMetaInfo(dos);

            dos.flush();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @throws IgniteCheckedException If fails.
     */
    public void readAck() throws IgniteCheckedException {
        try {
            String ack = dis.readUTF();

            if (!ACK_MSG.equals(ack))
                throw new IOException("Incorrect ack message");
        }
        catch (EOFException e) {
            throw new IgniteCheckedException("Input connection closed unexpectedly", e);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @throws IOException If fails.
     */
    public void writeAck() throws IOException {
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
     * @param offset Offset to start writing to the file.
     * @param expected The number of bytes to expect.
     * @throws IgniteCheckedException If fails.
     */
    public void readInto(File file, long offset, long expected) throws IgniteCheckedException {
        // Nothing to read.
        if (expected <= 0)
            return;

        try {
            try (FileIO fileIO = dfltIoFactory.create(file, CREATE, WRITE)) {
                fileIO.position(offset);

                long size = expected;

                while (size > 0) {
                    long readed = fileIO.transferFrom(channel, fileIO.position(), size);

                    if (readed < 0)
                        break;

                    size -= readed;
                }
            }

            writeAck();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param buff Buffer to read data into.
     * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the channel has reached end-of-stream.
     * @throws IgniteCheckedException If fails.
     */
    public int readInto(ByteBuffer buff) throws IgniteCheckedException {
        try {
            return channel.read(buff);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param file Partition file.
     * @throws IgniteCheckedException If fails.
     */
    public void writeFrom(File file, long offset, long size) throws IgniteCheckedException {
        FileIO fileIO = null;

        // Nothing to send.
        if (size <= 0)
            return;

        try {
            fileIO = dfltIoFactory.create(file, READ);

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
