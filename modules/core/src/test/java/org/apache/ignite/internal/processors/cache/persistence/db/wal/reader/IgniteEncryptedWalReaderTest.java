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

import java.io.File;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.encryption.EncryptionSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.EncryptedRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.encryption.EncryptionSpiImpl;

import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.genNewStyleSubfolderName;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;

/** */
public class IgniteEncryptedWalReaderTest extends AbstractIgniteWalReaderTest {
    /** {@inheritDoc} */
    protected boolean encrypted() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected EncryptionSpi encryptionSpi() {
        EncryptionSpiImpl encSpi = new EncryptionSpiImpl();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        return encSpi;
    }

    public void testFillWalAndReadRecords() throws Exception {
        setWalAndArchiveToSameValue = false;
        int cacheObjectsToWrite = 1000;

        Ignite ignite0 = startGrid("node0");

        ignite0.active(true);

        putDummyRecords(ignite0, cacheObjectsToWrite);

        String nodeFolder = genNewStyleSubfolderName(0, (UUID)ignite0.cluster().localNode().consistentId());

        stopGrid("node0", false);

        String workDir = U.defaultWorkDirectory();
        File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        File wal = new File(db, "wal");

        File walWorkDirWithConsistentId = new File(wal, nodeFolder);
        IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, nodeFolder);

        int encRecCnt = 0;

        try (WALIterator walIter = factory.iteratorWorkFiles(
                walWorkDirWithConsistentId.listFiles(WAL_SEGMENT_FILE_FILTER))) {
            for (IgniteBiTuple<WALPointer, WALRecord> next : walIter) {
                WALRecord walRecord = next.get2();

                if (walRecord.type() == ENCRYPTED_RECORD &&
                    ((EncryptedRecord)walRecord).clearRecordType() == DATA_RECORD)
                    encRecCnt++;
            }
        }

        assertEquals(cacheObjectsToWrite, encRecCnt);
    }
}
