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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Part of direct node to node file downloading
 */
public class FileUploader {
    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

    /** */
    private final Path path;

    /** */
    private final IgniteLogger log;

    /**
     *
     */
    public FileUploader(Path path, IgniteLogger log) {
        this.path = path;
        this.log = log;
    }

    /**
     *
     */
    public void upload(SocketChannel writeChan, GridFutureAdapter<Long> finishFut) {
        FileChannel readChan = null;

        try {
            File file = new File(path.toUri().getPath());

            if (!file.exists()) {
                finishFut.onDone(
                    new IgniteCheckedException(
                        new FileNotFoundException(file.getAbsolutePath())
                    )
                );

                return;
            }

            readChan = FileChannel.open(path, StandardOpenOption.READ);

            long written = 0;

            long size = readChan.size();

            while (written < size)
                written += readChan.transferTo(written, CHUNK_SIZE, writeChan);

            writeChan.shutdownOutput();
            writeChan.shutdownInput();

            finishFut.onDone(written);
        }
        catch (IOException ex) {
            finishFut.onDone(ex);
        }
        finally {
            U.close(writeChan, log);
            U.close(readChan, log);
        }
    }
}
