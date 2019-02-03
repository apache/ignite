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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/** */
public class FileIoDownloader extends AbstractFileIoConnector {
    /** */
    private final File downloadDir;

    /** */
    public FileIoDownloader(
        SocketChannel channel,
        FileIOFactory factory,
        IgniteLogger log,
        File downloadDir
    ) throws IOException {
        super(channel, factory, log);

        this.downloadDir = downloadDir;
    }

    /** */
    public File download() throws IOException {
        T2<String, Long> ioMeta = readFileMeta();

        U.log(log, "Start downloading file [downloadDir=" + downloadDir.getPath() +
            ", name=" + ioMeta.get1() + ", size=" + ioMeta.get2() + ']');

        File file = new File(downloadDir, ioMeta.get1() + ".tmp");

        try (FileIO fileIO = factory.create(file, CREATE, WRITE)) {
            long count = ioMeta.get2();

            while (count > 0) {
                long written = fileIO.transferFrom(channel, fileIO.position(), count);

                if (written < 0)
                    break;

                count -= written;
            }
        }

        writeFileMeta(ioMeta.get1(), ioMeta.get2());

        return file;
    }
}
