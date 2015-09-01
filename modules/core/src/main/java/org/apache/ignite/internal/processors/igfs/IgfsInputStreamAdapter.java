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

package org.apache.ignite.internal.processors.igfs;

import java.io.IOException;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;

/**
 * Implementation adapter providing necessary methods.
 */
public abstract class IgfsInputStreamAdapter extends IgfsInputStream
    implements IgfsSecondaryFileSystemPositionedReadable {
    /** {@inheritDoc} */
    @Override public long length() {
        return fileInfo().length();
    }

    /**
     * Gets file info for opened file.
     *
     * @return File info.
     */
    public abstract IgfsFileInfo fileInfo();

    /**
     * Reads bytes from given position.
     *
     * @param pos Position to read from.
     * @param len Number of bytes to read.
     * @return Array of chunks with respect to chunk file representation.
     * @throws IOException If read failed.
     */
    public abstract byte[][] readChunks(long pos, int len) throws IOException;
}