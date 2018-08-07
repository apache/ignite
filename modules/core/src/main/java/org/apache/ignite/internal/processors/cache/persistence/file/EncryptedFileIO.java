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
import org.apache.ignite.encryption.EncryptionSpi;
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
    private EncryptionKey key;

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

        this.encryptionOverhead = encSpi.encryptedSize(pageSize) - pageSize;
        this.zeroes =  new byte[encryptionOverhead];
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
        assert destBuf.capacity() == pageSize;
        assert position() != 0;

        ByteBuffer encrypted = ByteBuffer.allocate(pageSize);

        int res = plainFileIO.read(encrypted, position);

        if (res < 0)
            return res;

        if (res != pageSize) {
            throw new IllegalStateException("Expecting to read whole page[" + pageSize + " bytes], " +
                "but read only " + res + " bytes");
        }

        destBuf.put(encSpi.decrypt(encrypted.array(), key()));
        destBuf.put(zeroes); //We should ensure

        return res;
    }

    /** {@inheritDoc} */
    @Override public int readFully(ByteBuffer destBuf, long position) throws IOException {
        return read(destBuf, position);
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
        assert position() != 0;
        assert srcBuf.capacity() == pageSize;

        byte[] srcArr = new byte[pageSize];

        srcBuf.get(srcArr, 0, pageSize);

        assert tailIsEmpty(srcArr);

        byte[] encrypted = encSpi.encrypt(srcArr, key(), 0, srcArr.length - encryptionOverhead);

        return plainFileIO.write(ByteBuffer.wrap(encrypted), position);
    }

    /** */
    private boolean tailIsEmpty(byte[] srcArr) {
        for (int i = srcArr.length - encryptionOverhead; i<srcArr.length; i++)
            assert srcArr[i] == 0 : "Tail of srcArr should be empty - " + i;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
        return write(srcBuf, position);
    }

    /**
     * @return Encryption key.
     */
    private EncryptionKey key() {
        if (key == null)
            return key = encMgr.groupKey(groupId);

        return key;
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
