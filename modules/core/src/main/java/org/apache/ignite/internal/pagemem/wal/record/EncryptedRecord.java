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

package org.apache.ignite.internal.pagemem.wal.record;

/**
 * Encrypted record from WAL.
 * That types of record returned from a {@code RecordDataSerializer} on offline WAL iteration.
 */
public class EncryptedRecord extends WALRecord implements WalRecordCacheGroupAware {
    /**
     * Group id.
     */
    private int grpId;

    /**
     * Type of plain record.
     */
    private RecordType plainRecType;

    /**
     * @param grpId Group id
     * @param plainRecType Plain record type.
     */
    public EncryptedRecord(int grpId, RecordType plainRecType) {
        this.grpId = grpId;
        this.plainRecType = plainRecType;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.ENCRYPTED_RECORD;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grpId;
    }

    /**
     * @return Type of plain record.
     */
    public RecordType plainRecordType() {
        return plainRecType;
    }
}
