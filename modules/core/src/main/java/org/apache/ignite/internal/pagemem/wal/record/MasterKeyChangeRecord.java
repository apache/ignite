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

import java.util.Map;
import java.util.Map.Entry;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD;

/**
 * Logical record that stores encryption keys. Written to the WAL on the master key change.
 */
public class MasterKeyChangeRecord extends WALRecord {
    /** Master key name. */
    private final String masterKeyName;

    /** Group keys encrypted by the master key. */
    private final Map<Integer, byte[]> grpKeys;

    /**
     * @param masterKeyName Master key name.
     * @param grpKeys Encrypted group keys.
     */
    public MasterKeyChangeRecord(String masterKeyName, Map<Integer, byte[]> grpKeys) {
        this.masterKeyName = masterKeyName;
        this.grpKeys = grpKeys;
    }

    /** @return Master key name. */
    public String getMasterKeyName() {
        return masterKeyName;
    }

    /** @return Encrypted group keys. */
    public Map<Integer, byte[]> getGrpKeys() {
        return grpKeys;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return MASTER_KEY_CHANGE_RECORD;
    }

    /** @return Record data size. */
    public int dataSize() {
        int size = /*Master key name length*/4 + masterKeyName.getBytes().length + /*Group keys map size*/4;

        for (Entry<Integer, byte[]> entry : grpKeys.entrySet())
            size += /*grpId*/4 + /*grp key size*/4 + entry.getValue().length;

        return size;
    }
}
