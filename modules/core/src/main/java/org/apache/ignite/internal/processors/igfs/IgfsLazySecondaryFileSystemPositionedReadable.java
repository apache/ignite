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

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.IOException;

/**
 * Lazy readable entity which is opened on demand.
 */
public class IgfsLazySecondaryFileSystemPositionedReadable implements IgfsSecondaryFileSystemPositionedReadable {
    /** File system. */
    private final IgfsSecondaryFileSystem fs;

    /** Path. */
    private final IgfsPath path;

    /** Buffer size. */
    private final int bufSize;

    /** Target stream. */
    private IgfsSecondaryFileSystemPositionedReadable target;

    /**
     * Constructor.
     *
     * @param fs File system.
     * @param path Path.
     * @param bufSize Buffer size.
     */
    public IgfsLazySecondaryFileSystemPositionedReadable(IgfsSecondaryFileSystem fs, IgfsPath path, int bufSize) {
        assert fs != null;
        assert path != null;

        this.fs = fs;
        this.path = path;
        this.bufSize = bufSize;
    }

    /** {@inheritDoc} */
    @Override public int read(long pos, byte[] buf, int off, int len) throws IOException {
        if (target == null)
            target = fs.open(path, bufSize);

        return target.read(pos, buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (target != null)
            target.close();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsLazySecondaryFileSystemPositionedReadable.class, this);
    }
}
