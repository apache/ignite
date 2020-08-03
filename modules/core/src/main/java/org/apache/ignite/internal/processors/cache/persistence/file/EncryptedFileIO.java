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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.CU;
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
     * Size of file header in bytes.
     */
    private final int headerSize;

    /**
     * Shared database manager.
     */
    private final GridEncryptionManager encMgr;

    /**
     * Shared database manager.
     */
    private final EncryptionSpi encSpi;

    /**
     * Encryption key.
     */
    private Serializable encKey;

    /**
     * Extra bytes added by encryption.
     */
    private final int encryptionOverhead;

    /**
     * Array of zeroes to fulfill tail of decrypted page.
     */
    private final byte[] zeroes;

    /**
     * @param plainFileIO Underlying file.
     * @param groupId Group id.
     * @param pageSize Size of plain data page in bytes.
     * @param headerSize Size of file header in bytes.
     * @param encMgr Encryption manager.
     */
    EncryptedFileIO(FileIO plainFileIO, int groupId, int pageSize, int headerSize,
        GridEncryptionManager encMgr, EncryptionSpi encSpi) {
        this.plainFileIO = plainFileIO;
        this.groupId = groupId;
        this.pageSize = pageSize;
        this.headerSize = headerSize;
        this.encMgr = encMgr;
        this.encSpi = encSpi;

        this.encryptionOverhead = pageSize - CU.encryptedPageSize(pageSize, encSpi);
        this.zeroes = new byte[encryptionOverhead];
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
        assert position() == 0;

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
        assert position() != 0;

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
        assert position() == 0;
        assert headerSize == srcBuf.capacity();

        return plainFileIO.write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
        return write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        encrypt(srcBuf, encrypted);

        encrypted.rewind();

        return plainFileIO.write(encrypted, position);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        encrypt(srcBuf, encrypted);

        encrypted.rewind();

        return plainFileIO.writeFully(encrypted, position);
    }

    /**
     * @param srcBuf Source buffer.
     * @param res Destination buffer.
     * @throws IOException If failed.
     */
    private void encrypt(ByteBuffer srcBuf, ByteBuffer res) throws IOException {
        assert position() != 0;
        assert srcBuf.remaining() >= pageSize;
        assert tailIsEmpty(srcBuf, PageIO.getType(srcBuf));

        int srcLimit = srcBuf.limit();

        srcBuf.limit(srcBuf.position() + plainDataSize());

        encSpi.encryptNoPadding(srcBuf, key(), res);

        res.rewind();

        storeCRC(res);

        srcBuf.limit(srcLimit);
        srcBuf.position(srcBuf.position() + encryptionOverhead);
    }

    /**
     * @param encrypted Encrypted buffer.
     * @param destBuf Destination buffer.
     */
    private void decrypt(ByteBuffer encrypted, ByteBuffer destBuf) throws IOException {
        assert encrypted.remaining() >= pageSize;
        assert encrypted.limit() >= pageSize;

        checkCRC(encrypted);

        encrypted.limit(encryptedDataSize());

        encSpi.decryptNoPadding(encrypted, key(), destBuf);

        destBuf.put(zeroes); //Forcibly purge page buffer tail.
    }

    /**
     * Stores CRC in res.
     *
     * @param res Destination buffer.
     */
    private void storeCRC(ByteBuffer res) {
        int crc = FastCrc.calcCrc(res, encryptedDataSize());

        res.put((byte) (crc >> 24));
        res.put((byte) (crc >> 16));
        res.put((byte) (crc >> 8));
        res.put((byte) crc);
    }

    /**
     * Checks encrypted data integrity.
     *
     * @param encrypted Encrypted data buffer.
     */
    private void checkCRC(ByteBuffer encrypted) throws IOException {
        int crc = FastCrc.calcCrc(encrypted, encryptedDataSize());

        int storedCrc = 0;

        storedCrc |= (int)encrypted.get() << 24;
        storedCrc |= ((int)encrypted.get() & 0xff) << 16;
        storedCrc |= ((int)encrypted.get() & 0xff) << 8;
        storedCrc |= encrypted.get() & 0xff;

        if (crc != storedCrc) {
            throw new IOException("Content of encrypted page is broken. [StoredCrc=" + storedCrc +
                ", calculatedCrd=" + crc + "]");
        }

        encrypted.position(encrypted.position() - (encryptedDataSize() + 4 /* CRC size. */));
    }

    /**
     * @return Encrypted data size.
     */
    private int encryptedDataSize() {
        return pageSize - encSpi.blockSize();
    }

    /**
     * @return Plain data size.
     */
    private int plainDataSize() {
        return pageSize - encryptionOverhead;
    }

    /** */
    private boolean tailIsEmpty(ByteBuffer src, int pageType) {
        int srcPos = src.position();

        src.position(srcPos + plainDataSize());

        for (int i = 0; i < encryptionOverhead; i++)
            assert src.get() == 0 : "Tail of src should be empty [i=" + i + ", pageType=" + pageType + "]";

        src.position(srcPos);

        return true;
    }

    /**
     * @return Encryption key.
     */
    private Serializable key() {
        if (encKey == null)
            return encKey = encMgr.groupKey(groupId);

        return encKey;
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
