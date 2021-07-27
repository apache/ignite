/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.jraft.error.RetryAgainException;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;

/**
 * Read a file data form local dir by fileName.
 */
public class LocalDirReader implements FileReader {

    private static final IgniteLogger LOG = IgniteLogger.forClass(LocalDirReader.class);

    private final String path;

    public LocalDirReader(String path) {
        super();
        this.path = path;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public int readFile(final ByteBufferCollector buf, final String fileName, final long offset, final long maxCount)
        throws IOException,
        RetryAgainException {
        return readFileWithMeta(buf, fileName, null, offset, maxCount);
    }

    @SuppressWarnings("unused")
    protected int readFileWithMeta(final ByteBufferCollector buf, final String fileName, final Message fileMeta,
        long offset, final long maxCount) throws IOException, RetryAgainException {
        buf.expandIfNecessary();
        final String filePath = this.path + File.separator + fileName;
        final File file = new File(filePath);
        try (final FileInputStream input = new FileInputStream(file); final FileChannel fc = input.getChannel()) {
            int totalRead = 0;
            while (true) {
                final int nread = fc.read(buf.getBuffer(), offset);
                if (nread <= 0) {
                    return EOF;
                }
                totalRead += nread;
                if (totalRead < maxCount) {
                    if (buf.hasRemaining()) {
                        return EOF;
                    }
                    else {
                        buf.expandAtMost((int) (maxCount - totalRead));
                        offset += nread;
                    }
                }
                else {
                    final long fsize = file.length();
                    if (fsize < 0) {
                        LOG.warn("Invalid file length {}", filePath);
                        return EOF;
                    }
                    if (fsize == offset + nread) {
                        return EOF;
                    }
                    else {
                        return totalRead;
                    }
                }
            }
        }
    }
}
