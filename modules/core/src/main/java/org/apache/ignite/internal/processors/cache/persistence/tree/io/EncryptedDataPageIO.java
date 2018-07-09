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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.encryption.EncryptionKey;
import org.apache.ignite.spi.encryption.EncryptionSpiImpl;

/**
 * Encrypted data pages IO.
 */
public class EncryptedDataPageIO extends DataPageIO {
    /** */
    public static final IOVersions<DataPageIO> VERSIONS = new IOVersions<>(
        new EncryptedDataPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private EncryptedDataPageIO(int ver) {
        super(T_ENCRYPTED_DATA, ver);
    }

    /** {@inheritDoc} */
    @Override protected void setEmptyPage(long pageAddr, int pageSize) {
        setEmptyPage0(pageAddr, pageSize, pageSize - shouldByReserved(pageSize));
    }

    /** {@inheritDoc} */
    @Override protected int actualFreeSpace(long pageAddr, int pageSize) {
        return super.actualFreeSpace(pageAddr, pageSize) - shouldByReserved(pageSize);
    }

    /**
     * Calculates count of bytes that should be reserved in each page for encryption purposes.
     * {@code encryptedSize(pageSize - shouldByReserved(pageSize)) == pageSize}.
     * We want to write {@code pageSize} bytes to the disk to perform optimally.
     *
     * @param pageSize Page size.
     * @return Count of bytes that should be reserved.
     * @see EncryptionSpiImpl#encrypt(byte[], EncryptionKey, int, int)
     * @see EncryptionSpiImpl#encryptedSize(int)
     */
    private int shouldByReserved(int pageSize) {
        return EncryptionSpiImpl.encryptedSize0(pageSize) - pageSize;
    }
}
