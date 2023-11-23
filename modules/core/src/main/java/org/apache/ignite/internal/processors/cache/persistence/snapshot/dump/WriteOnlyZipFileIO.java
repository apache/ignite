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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.internal.processors.cache.persistence.file.AbstractFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.internal.A;

import static java.util.zip.Deflater.BEST_COMPRESSION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.ZIP_SUFFIX;

/**
 * {@link FileIO} that allows to write ZIP compressed file.
 * It doesn't support reading or random access.
 * It is not designed for writing concurrently from several threads.
 */
public class WriteOnlyZipFileIO extends AbstractFileIO {
    /** */
    private final ZipOutputStream zos;

    /** */
    private final WritableByteChannel ch;

    /** */
    private long pos;

    /** */
    public WriteOnlyZipFileIO(File file) throws IOException {
        A.ensure(file.getName().endsWith(ZIP_SUFFIX), "File name should end with " + ZIP_SUFFIX);

        String entryName = file.getName().substring(0, file.getName().length() - ZIP_SUFFIX.length());

        zos = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(Paths.get(file.getPath()))));

        zos.setLevel(BEST_COMPRESSION);

        zos.putNextEntry(new ZipEntry(entryName));

        ch = Channels.newChannel(zos);
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return pos;
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        int written = ch.write(srcBuf);

        pos += written;

        return written;
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        force(false);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        zos.closeEntry();

        ch.close();
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long position, int len) {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return -1;
    }
}
