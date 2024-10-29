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

import java.util.Iterator;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * This record notifies {@link CdcMain} about committed WAL state. It's assumed that WAL state will be inferred from
 * {@link CdcConsumer} committed WALPointer.
 *
 * @see CdcConsumer#onEvents(Iterator)
 */
public class CdcManagerRecord extends WALRecord {
    /** CDC consumer WAL state. */
    private final T2<WALPointer, Integer> state;

    /** */
    public CdcManagerRecord(T2<WALPointer, Integer> state) {
        this.state = state;
    }

    /** @return CDC consumer WAL state. */
    public T2<WALPointer, Integer> walState() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CDC_MANAGER_RECORD;
    }
}
