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
import org.jetbrains.annotations.Nullable;

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
     * Size of file header in bytes.
     */
    private final int headerSize;

    /**
     * Encryption key provider.
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
     * @param headerSize Size of file header in bytes.
     * @param keyProvider Encryption key provider.
     */
    EncryptedFileIO(FileIO plainFileIO, int groupId, int pageSize, int headerSize, EncryptionCacheKeyProvider keyProvider,
        EncryptionSpi encSpi) {
        this.plainFileIO = plainFileIO;
        this.groupId = groupId;
        this.pageSize = pageSize;
        this.headerSize = headerSize;
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
        assert position() == 0 && headerSize > 0;

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
        assert position() >= headerSize;

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
        if (headerSize > 0) {
            assert position() == 0;
            assert headerSize == srcBuf.capacity();

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
     * @param srcBuf Source buffer.
     * @param res Destination buffer.
     * @throws IOException If failed.
     */
    private void encrypt(ByteBuffer srcBuf, ByteBuffer res) throws IOException {
        assert position() >= headerSize;

        try {
            encUtil.encrypt(srcBuf, res, grpKey(groupId, null));
        }
        catch (EncryptionKeyNotFoundException e) {
            throw new IOException("Failed to encrypt data for cache group " + groupId + '.', e);
        }
    }

    /**
     * Finds encryption key for cache group {@code grpId}.
     *
     * @param grpId Cache group id.
     * @param keyId Key id. If {@code null}, the active key is used.
     * @return Encryption key if found.
     * @throws {@code EncryptionKeyNotFoundException} if the key isn't found.
     */
    private GroupKey grpKey(int grpId, @Nullable Integer keyId) throws EncryptionKeyNotFoundException {
        GroupKey key = keyId == null ? keyProvider.getActiveKey(grpId) : keyProvider.groupKey(grpId, keyId);

        if (key == null)
            throw new EncryptionKeyNotFoundException(grpId, keyId);

        return key;
    }

    /**
     * @return Encrypted data.
     */
    private ByteBuffer encrypt(ByteBuffer srcBuf) throws IOException {
        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        encrypt(srcBuf, encrypted);

        encrypted.rewind();

        return encrypted;
    }

    /**
     * @param encrypted Encrypted buffer.
     * @param destBuf Destination buffer.
     */
    private void decrypt(ByteBuffer encrypted, ByteBuffer destBuf) throws IOException {
        int keyId = encrypted.get(encryptedDataSize() + 4 /* CRC size. */) & 0xff;

        try {
            encUtil.decrypt(encrypted, destBuf, grpKey(groupId, keyId));
        }
        catch (EncryptionKeyNotFoundException e) {
            throw new IOException("Faled to decrypt data for cache group " + groupId + '.', e);
        }
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
