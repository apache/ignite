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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import org.apache.ignite.encryption.EncryptionSpi;
import org.apache.ignite.spi.encryption.AESEncryptionSpiImpl;

import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;

/** */
public class IgniteEncryptedWalReaderTest extends IgniteWalReaderTest {
    /** {@inheritDoc} */
    protected boolean encrypted() {
        return true;
    }

    /** {@inheritDoc} */
    protected EncryptionSpi encryptionSpi() {
        AESEncryptionSpiImpl encSpi = new AESEncryptionSpiImpl();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        return encSpi;
    }

    /** {@inheritDoc} */
    @Override public void testPutAllTxIntoTwoNodes() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testTxFillWalAndExtractDataRecords() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testTxRecordsReadWoBinaryMeta() throws Exception {
        // No-op.
    }
}
