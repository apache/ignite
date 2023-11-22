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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerRecord;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;

/**
 * CDC manager responsible for logic of capturing data within Ignite node. There is a contract between {@link CdcManager}
 * and {@link CdcMain}. Communication between these components are built with WAL records. {@link CdcManager} can
 * write some records, while {@link CdcMain} must listen to them.
 * <ul>
 *     <li>
 *         This manager can write {@link CdcManagerRecord} that contains info about consumer state.
 *         This record will be handled by {@link CdcMain} the same way it handles {@link CdcConsumer#onEvents(Iterator)}
 *         when it returns {@code true}.
 *     </li>
 *     <li>
 *         This manager must write {@link CdcManagerStopRecord} after it stopped handling records by any reason.
 *         {@link CdcMain} will start consume records instead since {@link CdcManagerRecord#walState()} of
 *         last consumed {@link CdcManagerRecord}.
 *     </li>
 *     <li>
 *         Apache Ignite provides a default implementation - {@link CdcUtilityActiveCdcManager}. It will set the CDC mode to
 *         {@link CdcMode#CDC_UTILITY_ACTIVE} on node start up.
 *     </li>
 * </ul>
 *
 * Ignite can be extended with custom CDC manager. It's required to implement this interface, create own {@link PluginProvider}
 * that will return the implementation with {@link PluginProvider#createComponent(PluginContext, Class)} method.
 * The {@code Class} parameter in the method is {@code CdcManager} class.
 *
 * @see CdcConsumer#onEvents(Iterator)
 * @see CdcConsumerState#saveCdcMode(CdcMode)
 */
public interface CdcManager extends GridCacheSharedManager {
    /**
     * Callback is invoked once, after Ignite restores memory on start-up. It invokes before {@link IgniteWriteAheadLogManager}
     * starts writing WAL records amd before the first call of {@link #collect(ByteBuffer)}.
     *
     * Implementation suggestions:
     * <ul>
     *     <li>This method can be used for restoring CDC state on Ignite node start, collecting missed events from WAL segments.</li>
     *     <li>Be aware, this method runs in Ignite main thread and might lengthen the Ignite start procedure.</li>
     * </ul>
     */
    public void afterMemoryRestore() throws IgniteCheckedException;

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
     *          Content of the buffer will not be changed before this method returns.
     *          But Ignite doesn't guarantee it after the method finished.
     *      </li>
     *      <li>It must not block the calling thread and work quickly.</li>
     *      <li>Ignite logs and ignores any {@link Throwable} throwed from this method.</li>
     *  </ul>
     *
     * @param dataBuf Buffer that contains data to collect.
     */
    public void collect(ByteBuffer dataBuf);

    /**
     * If this manager isn't active then Ignite skips calls of {@link #afterMemoryRestore()} and {@link #collect(ByteBuffer)} methods.
     *
     * @return {@code true} if manager is active, otherwise {@code false}.
     */
    public boolean active();
}
