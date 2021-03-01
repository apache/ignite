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

import java.util.List;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD_V2;

/**
 * Logical record that stores encryption keys. Written to the WAL on the master key change.
 */
public class MasterKeyChangeRecordV2 extends WALRecord {
    /** Master key name. */
    private final String masterKeyName;

    /** Group keys encrypted by the master key. */
    private final List<T2<Integer, GroupKeyEncrypted>> grpKeys;

    /**
     * @param masterKeyName Master key name.
     * @param grpKeys Encrypted group keys.
     */
    public MasterKeyChangeRecordV2(String masterKeyName, List<T2<Integer, GroupKeyEncrypted>> grpKeys) {
        this.masterKeyName = masterKeyName;
        this.grpKeys = grpKeys;
    }

    /** @return Master key name. */
    public String getMasterKeyName() {
        return masterKeyName;
    }

    /** @return Encrypted group keys. */
    public List<T2<Integer, GroupKeyEncrypted>> getGrpKeys() {
        return grpKeys;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return MASTER_KEY_CHANGE_RECORD_V2;
    }

    /** @return Record data size. */
    public int dataSize() {
        int size = /*Master key name length*/4 + masterKeyName.getBytes().length + /*list size*/4;

        for (T2<Integer, GroupKeyEncrypted> entry : grpKeys)
            size += /*grpId*/4 + /*grp key size*/4 + /*grp key id size*/1 + entry.get2().key().length;

        return size;
    }
}

