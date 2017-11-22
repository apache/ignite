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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Part of direct node to node file downloading
 */
public class FileUploader {
    private static final int CHUNK_SIZE = 1024 * 1024;

    private final Path path;

    private final Executor exec;

    private final SocketChannel writeChannel;

    private final GridFutureAdapter<Long> fut = new GridFutureAdapter<>();

    public FileUploader(
        Path path,
        Executor exec,
        SocketChannel writeChannel
    ) {
        this.path = path;
        this.exec = exec;
        this.writeChannel = writeChannel;
    }

    public IgniteInternalFuture<Long> upload() {
        exec.execute(new Worker());

        return fut;
    }

    private class Worker implements Runnable {
        @Override public void run() {
            FileChannel readChannel = null;
            SocketChannel writeChannel = FileUploader.this.writeChannel;

            try {
                readChannel = FileChannel.open(path, StandardOpenOption.READ);

                long written = 0;

                long size = readChannel.size();

                while (written < size)
                    written += readChannel.transferTo(written, CHUNK_SIZE, writeChannel);

                fut.onDone(written);
            }
            catch (IOException ex) {
                fut.onDone(ex);
            }
            finally {
                try {
                    if (writeChannel != null)
                        writeChannel.close();
                }
                catch (IOException ex) {
                    throw new IgniteException("Could not close socket.");
                }

                try {
                    if (readChannel != null)
                        readChannel.close();
                }
                catch (IOException ex) {
                    throw new IgniteException("Could not close file: " + path);
                }
            }
        }
    }
}
