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
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.EncryptionSpi;

/** Encryption utililty class. */
public class EncryptionUtil {
    /** Encryption SPI. */
    private final EncryptionSpi encSpi;

    /** Page size. */
    private final int pageSize;

    /** Extra bytes added by encryption. */
    private final int encryptionOverhead;

    /** Array of zeroes to fulfill tail of decrypted page. */
    private final byte[] zeroes;

    /**
     * @param spi Spi.
     * @param pageSize Page size.
     */
    public EncryptionUtil(EncryptionSpi spi, int pageSize) {
        encSpi = spi;
        this.pageSize = pageSize;

        this.encryptionOverhead = pageSize - CU.encryptedPageSize(pageSize, encSpi);
        this.zeroes = new byte[encryptionOverhead];
    }

    /**
     * @param srcBuf Source buffer.
     * @param res Destination buffer.
     * @param grpKey Encryption key.
     */
    public void encrypt(ByteBuffer srcBuf, ByteBuffer res, GroupKey grpKey) {
        assert srcBuf.remaining() >= pageSize :
            "The number of elements remaining in buffer should be more or equal to page size " +
                "[srcBuf.remaining() = " + srcBuf.remaining() + ", pageSize = " + pageSize + "]";
        assert tailIsEmpty(srcBuf, PageIO.getType(srcBuf));

        int srcLimit = srcBuf.limit();

        srcBuf.limit(srcBuf.position() + plainDataSize());

        encSpi.encryptNoPadding(srcBuf, grpKey.key(), res);

        res.rewind();

        storeCRC(res);

        res.put(grpKey.id());

        srcBuf.limit(srcLimit);
        srcBuf.position(srcBuf.position() + encryptionOverhead);
    }

    /**
     * @param encrypted Encrypted buffer.
     * @param destBuf Destination buffer.
     */
    public void decrypt(ByteBuffer encrypted, ByteBuffer destBuf, GroupKey grpKey) throws IOException {
        assert encrypted.remaining() >= pageSize :
            "The number of elements remaining in encrypted buffer should be more or equal to page size " +
                "[encrypted.remaining() = " + encrypted.remaining() + ", pageSize = " + pageSize + "]";
        assert encrypted.limit() >= pageSize :
            "The limit of the encrypted buffer should be more or equal to page size " +
                "[encrypted.limit() = " + encrypted.limit() + ", pageSize = " + pageSize + "]";

        int crc = FastCrc.calcCrc(encrypted, encryptedDataSize());

        int storedCrc = 0;

        storedCrc |= (int)encrypted.get() << 24;
        storedCrc |= ((int)encrypted.get() & 0xff) << 16;
        storedCrc |= ((int)encrypted.get() & 0xff) << 8;
        storedCrc |= encrypted.get() & 0xff;

        if (crc != storedCrc) {
            throw new IOException("Content of encrypted page is broken. [StoredCrc=" + storedCrc +
                ", calculatedCrc=" + crc + "]");
        }

        encrypted.position(encrypted.position() - (encryptedDataSize() + 4 /* CRC size. */));

        encrypted.limit(encryptedDataSize());

        encSpi.decryptNoPadding(encrypted, grpKey.key(), destBuf);

        destBuf.put(zeroes); //Forcibly purge page buffer tail.
    }

    /**
     * Stores CRC in res.
     *
     * @param res Destination buffer.
     */
    private void storeCRC(ByteBuffer res) {
        int crc = FastCrc.calcCrc(res, encryptedDataSize());

        res.put((byte)(crc >> 24));
        res.put((byte)(crc >> 16));
        res.put((byte)(crc >> 8));
        res.put((byte)crc);
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

    /**
     * Checks that bytes for encryptionOverhead equal to zero.
     */
    private boolean tailIsEmpty(ByteBuffer src, int pageType) {
        int srcPos = src.position();

        src.position(srcPos + plainDataSize());

        for (int i = 0; i < encryptionOverhead; i++)
            assert src.get() == 0 : "Tail of src should be empty [i=" + i + ", pageType=" + pageType + "]";

        src.position(srcPos);

        return true;
    }
}
