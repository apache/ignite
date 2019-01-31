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
import java.nio.channels.WritableByteChannel;
import java.nio.file.NoSuchFileException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.READ;

/** */
public class FileIOUploader {
    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

    /** */
    private final WritableByteChannel target;

    /** */
    private final FileIOFactory factory;

    /** */
    private final IgniteLogger log;

    /**
     * 4 int - partition id.
     * 8 long - file size.
     */
    private final ByteBuffer buff;

    /** */
    public FileIOUploader(WritableByteChannel target, FileIOFactory factory, IgniteLogger log) {
        this.target = target;
        this.factory = factory;
        this.log = log;

        buff = ByteBuffer.allocate(12);
        buff.order(ByteOrder.nativeOrder());
    }

    /**
     * @param partFile Partition file.
     * @param partId Partition identifier.
     * @throws IgniteCheckedException If fails.
     */
    public void upload(File partFile, int partId) throws IgniteCheckedException {
        FileIO fileIO = null;

        try {
            fileIO = factory.create(partFile, READ);

            long written = 0;

            long size = fileIO.size();

            //Send input file length to server.
            buff.clear();

            buff.putInt(partId);
            buff.putLong(size);
            buff.flip();

            if (log.isInfoEnabled())
                log.info("Sending file metadata [partFile=" + partFile + ", partId=" + partId + ", size=" + size + ']');

            target.write(buff);

            // Send the whole file to channel.
            // Todo limit thransfer speed
            while (written < size)
                written += fileIO.transferTo(written, CHUNK_SIZE, target);

            if (log.isInfoEnabled())
                log.info("File transferred successfully to the corresponding channel.");
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            U.closeQuiet(fileIO);
        }
    }
}
