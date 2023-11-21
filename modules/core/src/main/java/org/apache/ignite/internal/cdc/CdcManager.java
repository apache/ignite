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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerRecord;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.plugin.PluginProvider;
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
 *         {@link CdcMain} should start consume records instead since {@link CdcManagerRecord#walState()} of
 *         last consumed {@link CdcManagerRecord}.
 *     </li>
 *     <li>
 *         The current behavior of {@link CdcManager} is persisted within {@link CdcConsumerState#saveCdcMode(CdcMode)}.
 *         After the mode set to {@link CdcMode#CDC_UTILITY_ACTIVE} it will not reset.
 *     </li>
 * </ul>
 *
 * Ignite can be extended with custom CDC manager implemented with {@link PluginProvider}.
 *
 * @see CdcConsumer#onEvents(Iterator)
 * @see CdcConsumerState#saveCdcMode(CdcMode)
 */
public interface CdcManager extends GridCacheSharedManager {
    /**
     * Collects byte buffer contains WAL records. Provided buffer {@code dataBuf} is a continuous part of WAL segment file.
     * The buffer might contain full content of a segment or only piece of it. There are guarantees:
     * <ul>
     *     <li>
     *         This method invokes sequentially, the provided buffer in the specified bounds of {@code off} and {@code limit}
     *         is a continuation of the previous one.
     *     </li>
     *     <li>The {@code dataBuf} is a read-only buffer.</li>
     * </ul>
     *
     * Implementation suggestions:
     *  <ul>
     *      <li>
     *          Frequence of calling the method depends on frequence of fsyncing WAL segment.
     *          See {@link IgniteSystemProperties#IGNITE_WAL_SEGMENT_SYNC_TIMEOUT}.
     *      </li>
     *      <li>
     *          It must handle the content of the {@code dataBuf} within the calling thread.
     *          There is a guarantee that content of the buffer will not change before this method finished.
     *          But Ignite doesn't guarantee it after the method finished.
     *      </li>
     *      <li>It must not block the calling thread and work quickly.</li>
     *      <li>Ignite logs and ignores any {@link Throwable} throwed from this method.</li>
     *  </ul>
     *
     * @param dataBuf Buffer that contains data to collect.
     * @param off Offset of the data within the buffer.
     * @param limit Limit of the data within the buffer.
     */
    public void collect(ByteBuffer dataBuf, int off, int limit);

    /**
     * Callback before WAL resumes logging. The method might be invoked few times on Ignite node start-up and Ignite
     * cluster activation. WALPointer {@code ptr} points to the head of the WAL, or {@code null} if no WAL records had written yet.
     *
     * The value of the pointer should be used for validating the collected data is full and checking its integrity.
     * The data before this pointer might not be collected with {@link #collect(ByteBuffer, int, int)} method and can
     * be read from WAL segments stored on the disk.
     *
     * There is a guarantee that all data after this pointer will be collected with {@link #collect(ByteBuffer, int, int)} method.
     *
     * @param ptr Pointer to the current WAL head record, {@code null} if no WAL records had written yet.
     */
    public void beforeResumeLogging(@Nullable WALPointer ptr);
}
