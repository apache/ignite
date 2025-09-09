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
import org.apache.ignite.internal.managers.encryption.EncryptionCacheKeyProvider;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.spi.encryption.EncryptionSpi;

/**
 * Implementation of {@code FileIO} that supports encryption(decryption) of pages written(readed) to(from) file.
 *
 * @see EncryptedFileIOFactory
 */
public class EncryptedFileIO implements FileIO {
    /**
     * Underlying file.
     */
    private final FileIO plainFileIO;

    /**
     * Group id.
     */
    private final int groupId;

    /**
     * Size of plain data page in bytes.
     */
    private final int pageSize;

    /**
     * Size of file header in bytes which is never encrypted.
     */
    private final int plainHeaderSize;

    /**
     * Encryption keys provider.
     */
    private final EncryptionCacheKeyProvider keyProvider;

    /**
     * Shared database manager.
     */
    private final EncryptionSpi encSpi;

    /** Encryption utililty class. */
    private final EncryptionUtil encUtil;

    /**
     * @param plainFileIO Underlying file.
     * @param groupId Group id.
     * @param pageSize Size of plain data page in bytes.
     * @param plainHeaderSize Size of file header in bytes which is never encrypted.
     * @param keyProvider Encryption keys provider.
     */
    EncryptedFileIO(FileIO plainFileIO, int groupId, int pageSize, int plainHeaderSize, EncryptionCacheKeyProvider keyProvider,
        EncryptionSpi encSpi) {
        this.plainFileIO = plainFileIO;
        this.groupId = groupId;
        this.pageSize = pageSize;
        this.plainHeaderSize = plainHeaderSize;
        this.keyProvider = keyProvider;
        this.encSpi = encSpi;

        this.encUtil = new EncryptionUtil(encSpi, pageSize);
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long position, int len) {
        throw new UnsupportedOperationException();
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
        assert position() == 0 && plainHeaderSize > 0;

        return plainFileIO.read(destBuf);
    }

    /** {@inheritDoc} */
    @Override public int readFully(ByteBuffer destBuf) throws IOException {
        return read(destBuf);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        assert destBuf.remaining() >= pageSize;
        assert position() != 0;

        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        int res = plainFileIO.read(encrypted, position);

        if (res < 0)
            return res;

        if (res != pageSize) {
            throw new IllegalStateException("Expecting to read whole page[" + pageSize + " bytes], " +
                "but read only " + res + " bytes");
        }

        encrypted.rewind();

        decrypt(encrypted, destBuf);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readFully(ByteBuffer destBuf, long position) throws IOException {
        assert destBuf.capacity() == pageSize;
        assert position() >= plainHeaderSize;

        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        int res = plainFileIO.readFully(encrypted, position);

        if (res < 0)
            return res;

        if (res != pageSize) {
            throw new IllegalStateException("Expecting to read whole page[" + pageSize + " bytes], " +
                "but read only " + res + " bytes");
        }

        encrypted.rewind();

        decrypt(encrypted, destBuf);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Encrypted File doesn't support this operation");
    }

    /** {@inheritDoc} */
    @Override public int readFully(byte[] buf, int off, int len) throws IOException {
        return read(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        if (plainHeaderSize > 0) {
            assert position() == 0;
            assert plainHeaderSize == srcBuf.capacity();

            return plainFileIO.write(srcBuf);
        }
        else
            return plainFileIO.writeFully(encrypt(srcBuf));
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
        return write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        return plainFileIO.write(encrypt(srcBuf), position);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
        return plainFileIO.writeFully(encrypt(srcBuf), position);
    }

    /**
     * @return Encrypted data.
     */
    private ByteBuffer encrypt(ByteBuffer srcBuf) throws IOException {
        assert position() >= plainHeaderSize;

        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        GroupKey key = keyProvider.getActiveKey(groupId);

        assert key != null : "No active encryption key found for cache group " + groupId;

        encUtil.encrypt(srcBuf, encrypted, key);

        encrypted.rewind();

        return encrypted;
    }

    /**
     * @param encrypted Encrypted buffer.
     * @param destBuf Destination buffer.
     */
    private void decrypt(ByteBuffer encrypted, ByteBuffer destBuf) throws IOException {
        int keyId = encrypted.get(encryptedDataSize() + 4 /* CRC size. */) & 0xff;

        GroupKey key = keyProvider.groupKey(groupId, keyId);

        assert key != null : "No encryption key found for cache group " + groupId + " by key id " + keyId;

        encUtil.decrypt(encrypted, destBuf, key);
    }

    /**
     * @return Encrypted data size.
     */
    private int encryptedDataSize() {
        return pageSize - encSpi.blockSize();
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Encrypted File doesn't support this operation");
    }

    /** {@inheritDoc} */
    @Override public int writeFully(byte[] buf, int off, int len) throws IOException {
        return write(buf, off, len);
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
