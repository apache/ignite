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

package org.apache.ignite.internal.processors.igfs.secondary.local;

import java.io.FileInputStream;
import java.io.IOException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;

/**
 * Positioned readable interface for local secondary file system.
 */
public class LocalIgfsSecondaryFileSystemPositionedReadable implements IgfsSecondaryFileSystemPositionedReadable {
    /** File input stream */
    private final FileInputStream in;

    /**
     * @param in FileInputStream
     */
    public LocalIgfsSecondaryFileSystemPositionedReadable(FileInputStream in) {
        this.in = in;
    }

    /** {@inheritDoc} */
    @Override public int read(long readPos, byte[] buf, int off, int len) throws IOException {
        if (in == null)
            throw new IOException("Stream is closed.");

        in.getChannel().position(readPos);

        return in.read(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        in.close();
    }
}