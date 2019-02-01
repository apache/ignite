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
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class FileIODownloader {
    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

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

    /** */
    public File download() throws IgniteCheckedException {
        assert source.isBlocking();

        try {
            buff.clear();

            U.log(log, "Read file metadata [outDir=" + outDir.getPath() + ']');

            //Read input file properties
            long readResult = source.read(buff);

            if (readResult <= 0)
                throw new IgniteCheckedException("Unable to recieve file metadata from the remote node.");

            buff.flip();

            int partId = buff.getInt();
            long size = buff.getLong();

            U.log(log, "Start downloading file [outDir=" + outDir.getPath() + ", partId=" + partId +
                ", size=" + size + ']');

            File partFile = new File(getPartitionFile(outDir, partId).getAbsolutePath() + ".tmp");

            try (FileIO fileIO = factory.create(partFile, CREATE, WRITE)) {
                //Write partition file.
                long written = 0;

                while (written < size)
                    written += fileIO.transferFrom(source, written, CHUNK_SIZE);
            }

            return partFile;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to download specified file.", e);
        }
    }
}
