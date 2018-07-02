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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.apache.ignite.encryption.EncryptionKey;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;

/**
 * Implementation of {@code FileIO} that supports encryption(decryption) of pages written(readed) to(from) file.
 *
 * @see EncryptedFileIOFactory
 */
public class EncryptedFileIO implements FileIO {
    /**
     * Underlying file.
     */
    private FileIO plainFileIO;

    /**
     * Group id.
     */
    private int groupId;

    /**
     * Size of clear data page in bytes.
     */
    private int pageSize;

    /**
     * Size of page on the disk.
     */
    private int pageSizeOnDisk;

    /**
     * Size of file header in bytes.
     */
    private int headerSize;

    /**
     * Shared database manager.
     */
    private GridEncryptionManager encMgr;

    /**
     * Encryption key.
     */
    private EncryptionKey<?> key;

    /**
     * @param plainFileIO Underlying file.
     * @param groupId Group id.
     * @param pageSize Size of clear data page in bytes.
     * @param headerSize Size of file header in bytes.
     * @param encMgr Encryption manager.
     */
    EncryptedFileIO(FileIO plainFileIO, int groupId, int pageSize, int pageSizeOnDisk, int headerSize,
        GridEncryptionManager encMgr) {
        this.plainFileIO = plainFileIO;
        this.groupId = groupId;
        this.pageSize = pageSize;
        this.pageSizeOnDisk = pageSizeOnDisk;
        this.headerSize = headerSize;
        this.encMgr = encMgr;
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        return plainFileIO.position();
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        plainFileIO.position(newPosition);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        assert position() == 0;

        return plainFileIO.read(destBuf);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        assert destBuf.capacity() == pageSize;

        ByteBuffer encrypted = ByteBuffer.allocate(pageSizeOnDisk);

        int res = plainFileIO.read(encrypted, position);

        if (res < 0)
            return res;

        if (res != pageSizeOnDisk) {
            throw new IllegalStateException("Expecting to read whole page[" + pageSizeOnDisk + " bytes], " +
                "but read only " + res + " bytes");
        }

        destBuf.put(encMgr.spi().decrypt(encrypted.array(), key()));

        return res;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Encrypted File doesn't support this operation");
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        assert position() == 0;
        assert headerSize == srcBuf.capacity();

        return plainFileIO.write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        assert srcBuf.capacity() == pageSize;

        byte[] srcArr = new byte[pageSize];

        srcBuf.get(srcArr, 0, pageSize);

        byte[] encrypted = encMgr.spi().encrypt(srcArr, key());

        return plainFileIO.write(ByteBuffer.wrap(encrypted), position);
    }

    /**
     * @return Encryption key.
     */
    private EncryptionKey<?> key() {
        if (key == null)
            return key = encMgr.groupKey(groupId);

        return key;
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Encrypted File doesn't support this operation");
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        throw new UnsupportedOperationException("Encrypted File doesn't support this operation");
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        plainFileIO.force();
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        plainFileIO.force(withMetadata);
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return plainFileIO.size();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        plainFileIO.clear();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        plainFileIO.close();
    }
}
