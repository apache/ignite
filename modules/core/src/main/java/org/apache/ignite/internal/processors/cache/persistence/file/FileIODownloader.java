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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class FileIODownloader {
    /** */
    private final SocketChannel source;

    /** */
    private final FileIOFactory factory;

    /** */
    private final File outDir;

    /** */
    private final IgniteLogger log;

    /**
     * 4 int - partition id.
     * 8 long - file size.
     */
    private final ByteBuffer buff;

    /** */
    public FileIODownloader(SocketChannel source, FileIOFactory factory, File outDir, IgniteLogger log) {
        this.source = source;
        this.factory = factory;
        this.outDir = outDir;
        this.log = log;

        buff = ByteBuffer.allocate(12);
        buff.order(ByteOrder.nativeOrder());
    }

    /**
     * @return Recevied metadata info.
     * @throws IOException If fails.
     */
    public T2<Integer, Long> readMeta() throws IOException {
        buff.clear();

        U.log(log, "Reading file metadata from remote: " + source.getRemoteAddress());

        //Read input file properties
        long readResult = source.read(buff);

        if (readResult <= 0)
            throw new IOException("Unable to get file metadata from remote: " + source.getRemoteAddress());

        buff.flip();

        final int partId = buff.getInt();
        final long size = buff.getLong();

        return new T2<>(partId, size);
    }

    /** */
    public void writeMeta(int partId, long size) throws IOException {
        //Write response to the remote node.
        buff.clear();
        buff.putInt(partId);
        buff.putLong(size);
        buff.flip();

        U.log(log, "Sending file metadata to remote: " + source.getRemoteAddress());

        U.writeFully(source, buff);
    }

    /** */
    public File download() throws IgniteCheckedException {
        try {
            buff.clear();

            U.log(log, "Reading file metadata [outDir=" + outDir.getPath() + ']');

            //Read input file properties
            long readResult = source.read(buff);

            if (readResult <= 0)
                throw new IgniteCheckedException("Unable to recieve file metadata from the remote node.");

            buff.flip();

            final int partId = buff.getInt();
            final long size = buff.getLong();

            U.log(log, "Start downloading file [outDir=" + outDir.getPath() + ", partId=" + partId +
                ", size=" + size + ']');

            File partFile = new File(getPartitionFile(outDir, partId).getAbsolutePath() + ".tmp");

            try (FileIO fileIO = factory.create(partFile, CREATE, WRITE)) {
                long count = size;

                while (count > 0) {
                    long written = fileIO.transferFrom(source, fileIO.position(), count);

                    if (written < 0)
                        break;

                    count -= written;
                }
            }

            //Write response to the remote node.
            buff.clear();
            buff.putInt(partId);
            buff.putLong(partFile.length());
            buff.flip();

            U.log(log, "Sending reply with metadata within thread: " + Thread.currentThread().getName());

            U.writeFully(source, buff);

            return partFile;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to download specified file.", e);
        }
    }
}
