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
import java.nio.channels.SocketChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.READ;

/** */
public class FileIoUploader extends AbstractFileInterplayer {
    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

    /** */
    public FileIoUploader(SocketChannel channel, FileIOFactory factory, IgniteLogger log) throws IOException {
        super(channel, factory, log);
    }

    /**
     * @param file Partition file.
     * @throws IgniteCheckedException If fails.
     */
    public void upload(File file) throws IgniteCheckedException {
        FileIO fileIO = null;

        try {
            fileIO = factory.create(file, READ);

            long size = fileIO.size();
            String name = file.getName();

            writeFileMeta(name, size);

            // Send the whole file to channel.
            // Todo limit thransfer speed
            long written = 0;

            while (written < size)
                written += fileIO.transferTo(written, CHUNK_SIZE, channel);

            //Waiting for the writing response.
            T2<String, Long> ioMeta = readFileMeta();

            if (size != ioMeta.get2())
                throw new IgniteCheckedException("Incorrect response file size.");
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            U.closeQuiet(fileIO);
        }
    }
}
