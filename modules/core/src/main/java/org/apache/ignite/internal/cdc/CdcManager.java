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

package org.apache.ignite.internal.cdc;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerRecord;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.jetbrains.annotations.Nullable;

/**
 * CDC manager responsible for logic of capturing data within Ignite node. There is a contract between {@link CdcManager}
 * and {@link CdcMain}. Communication between these components are built with over WAL records. {@link CdcManager} can
 * write some records, while {@link CdcMain} must listen to them.
 * <ul>
 *     <li>
 *         This manager can write {@link CdcManagerRecord} that contains info about consumer state.
 *         This record must be handled by {@link CdcMain} the same way it handles {@link CdcConsumer#onEvents(Iterator)}
 *         when it returns {@code true}.
 *     </li>
 *     <li>
 *         This manager writes {@link CdcManagerStopRecord} after it stopped handling records by any reason.
 *         {@link CdcMain} should start consume records instead since last consumed {@link CdcManagerRecord}.
 *     </li>
 *     <li>
 *         The current behavior of {@link CdcManager} is persisted within {@link CdcConsumerState#saveCdcMode(CdcMode)}.
 *         After the mode set to {@link CdcMode#CDC_UTILITY_ACTIVE} it will not reset.
 *     </li>
 * </ul>
 *
 * @see CdcConsumer#onEvents(Iterator)
 * @see CdcConsumerState#saveCdcMode(CdcMode)
 */
public interface CdcManager extends GridCacheSharedManager {
    /**
     * Collects byte buffer contains WAL records.
     *
     * @param dataBuf Buffer that contains data to collect.
     * @param off Offset of the data within the buffer.
     * @param limit Limit of the data within the buffer.
     */
    public void collect(ByteBuffer dataBuf, int off, int limit);

    /**
     * Callback before WAL resumes logging.
     *
     * @param ptr Pointer to the WAL head to resume logging.
     */
    public void beforeResumeLogging(@Nullable WALPointer ptr);

    /**
     * Callback after WAL resumes logging.
     */
    public void afterResumeLogging();
}
