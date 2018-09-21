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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.CompressorFactory;

/**
 * {@link FileIO} that allows to work with compressed file.
 * Doesn't allow random access and setting {@link FileIO#position()} backwards.
 * Allows sequential reads including setting {@link FileIO#position()} forward.
 */
public class CompressorFileIO extends AbstractFileIO {
    /** Zip input stream. */
    private final InputStream zis;

    /** Byte array for draining data. */
    private final byte[] arr = new byte[128 * 1024];

    /** Total bytes read counter. */
    private long totalBytesRead = 0;

    /**
     * @param compressorFactory Factory to provide I/O interfaces for read/write operations with archive.
     * @param zip Compressed file.
     */
    public CompressorFileIO(CompressorFactory compressorFactory, File zip) throws IOException {
        zis = compressorFactory.decompress(new FileInputStream(zip));
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return totalBytesRead;
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        if (newPosition == totalBytesRead)
            return;

        if (newPosition < totalBytesRead)
            throw new UnsupportedOperationException("Seeking backwards is not supported.");

        long bytesRemaining = newPosition - totalBytesRead;

        while (bytesRemaining > 0) {
            int bytesToRead = bytesRemaining > arr.length ? arr.length : (int)bytesRemaining;

            bytesRemaining -= zis.read(arr, 0, bytesToRead);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer dstBuf) throws IOException {
        int bytesRead = zis.read(arr, 0, Math.min(dstBuf.remaining(), arr.length));

        if (bytesRead == -1)
            return -1;

        dstBuf.put(arr, 0, bytesRead);

        totalBytesRead += bytesRead;

        return bytesRead;
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer dstBuf, long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        throw new UnsupportedOperationException();
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
    @Override public void force() throws IOException {
        force(false);
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        throw new UnsupportedOperationException();
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
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        zis.close();
    }
}
