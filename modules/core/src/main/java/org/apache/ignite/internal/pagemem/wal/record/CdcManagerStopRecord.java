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

import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.cdc.CdcManager;

/**
 * Record notifies {@link CdcMain} that {@link CdcManager} fails and stop processing WAL data in-memory.
 * {@link CdcMain} must start capturing and process {@link CdcEvent} in regularly(active) way.
 */
public class CdcManagerStopRecord extends WALRecord {
    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CDC_MANAGER_STOP_RECORD;
    }
}
