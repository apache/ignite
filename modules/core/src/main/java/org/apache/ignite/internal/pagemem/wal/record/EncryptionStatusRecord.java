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

/**
 * Logical record to restart encryption with the latest encryption key.
 */
public class EncryptionStatusRecord extends WALRecord {
    /** Mapping the cache group ID to a list of partitions with the number of encrypted pages. */
    private final Map<Integer, List<T2<Integer, Integer>>> grpStates;

    /**
     * @param grpStates Mapping the cache group ID to a list of partitions with the number of encrypted pages.
     */
    public EncryptionStatusRecord(Map<Integer, List<T2<Integer, Integer>>> grpStates) {
        this.grpStates = grpStates;
    }

    /**
     * @return Mapping the cache group ID to a list of partitions with the number of encrypted pages.
     */
    public Map<Integer, List<T2<Integer, Integer>>> groupsStatus() {
        return grpStates;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.ENCRYPTION_STATUS_RECORD;
    }

    /** @return Record data size. */
    public int dataSize() {
        int size = 4;

        for (List list : grpStates.values())
            size += /*grpId*/4 + /*length*/4 + (list.size() * (/**partId*/2 + /*pagesCnt*/4));

        return size;
    }
}
