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

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD;

/**
 * Logical data record indented for master key change action.
 */
public class MasterKeyChangeRecord extends WALRecord {
    /** */
    private final String masterKeyId;

    /** */
    private final Map<Integer, byte[]> grpKeys;

    /**
     * @param masterKeyId Master key id.
     * @param grpKeys Encrypted cache keys.
     */
    public MasterKeyChangeRecord(String masterKeyId, Map<Integer, byte[]> grpKeys) {
        this.masterKeyId = masterKeyId;
        this.grpKeys = grpKeys;
    }

    /**
     * @return Master key id.
     */
    public String getMasterKeyId() {
        return masterKeyId;
    }

    /**
     * @return Encrypted cache keys.
     */
    public Map<Integer, byte[]> getGrpKeys() {
        return grpKeys;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return MASTER_KEY_CHANGE_RECORD;
    }

    /**
     * @return Record's data size.
     */
    public int dataSize() {
        int size = /*Master key id length*/4 + masterKeyId.length() + /*Group keys map size*/4;

        for (Map.Entry<Integer, byte[]> entry : grpKeys.entrySet())
            size += /*grpId*/4 + /*grp key size*/4 + entry.getValue().length;

        return size;
    }
}
