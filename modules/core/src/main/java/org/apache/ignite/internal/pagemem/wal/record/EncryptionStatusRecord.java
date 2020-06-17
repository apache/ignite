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
import java.util.Map;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTION_STATUS_RECORD;

public class EncryptionStatusRecord extends WALRecord {
    private final Map<Integer, List<T2<Integer, Integer>>> grpStates;

    public EncryptionStatusRecord(Map<Integer, List<T2<Integer, Integer>>> grpStates) {
        this.grpStates = grpStates;
    }

    public Map<Integer, List<T2<Integer, Integer>>> groupsStatus() {
        return grpStates;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return ENCRYPTION_STATUS_RECORD;
    }

    /** @return Record data size. */
    public int dataSize() {
        int size = 4;

        for (List entry : grpStates.values())
            size += /*grpId*/4 + /*length*/4 + (entry.size() * (2 + 4));

        return size;
    }

}
