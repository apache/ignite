/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInputImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Class that represents information about an encrypted WAL record or data entry.
 */
class DecryptionResult {
    /** Decrypted WAL record or {@code null} if it couldn't be decrypted. */
    @Nullable
    private final ByteBufferBackedDataInput decryptedData;

    /**
     * Type of the encrypted WAL record or {@code null} if this instance contains an encrypted data entry
     * and not a WAL record.
     */
    @Nullable
    private final WALRecord.RecordType recordType;

    /** Cache group id. */
    private final int grpId;

    /**
     * @param decryptedData Decrypted WAL record or {@code null} if it couldn't be decrypted.
     * @param recordType Type of the encrypted WAL record or {@code null}
     *                   if this instance contains an encrypted data entry and not a WAL record.
     * @param grpId Cache group id.
     */
    DecryptionResult(
        @Nullable ByteBuffer decryptedData,
        @Nullable WALRecord.RecordType recordType,
        int grpId
    ) {
        this.decryptedData = decryptedData == null ? null : new ByteBufferBackedDataInputImpl(decryptedData);
        this.recordType = recordType;
        this.grpId = grpId;
    }

    /**
     * Returns the decrypted WAL record or {@code null} if it couldn't be decrypted.
     */
    @Nullable ByteBufferBackedDataInput decryptedData() {
        return decryptedData;
    }

    /**
     * Returns the type of the encrypted WAL record or {@code null} if this instance contains an encrypted data entry
     * and not a WAL record.
     */
    @Nullable WALRecord.RecordType recordType() {
        return recordType;
    }

    /**
     * Returns cache group id that this record belongs to.
     */
    int grpId() {
        return grpId;
    }

    /**
     * Returns {@code true} if this instance contains decrypted data.
     */
    boolean isDecryptedSuccessfully() {
        return decryptedData != null;
    }
}
