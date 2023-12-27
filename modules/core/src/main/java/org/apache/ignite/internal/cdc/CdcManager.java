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
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerRecord;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;

/**
 * CDC manager responsible for logic of capturing data within Ignite node. Communication between {@link CdcManager} and
 * {@link CdcMain} components is established through CDC management WAL records. {@link CdcManager} logs them, while {@link CdcMain}
 * listens to them:
 * <ul>
 *     <li>
 *         This manager can log {@link CdcManagerRecord} with committed consumer state. This record will be handled by
 *         {@link CdcMain} the same way it handles {@link CdcConsumer#onEvents(Iterator)} when it returns {@code true}.
 *     </li>
 *     <li>
 *         This manager must log {@link CdcManagerStopRecord} after it stopped handling records by any reason.
 *         {@link CdcMain} will start consume records instead since {@link CdcManagerRecord#walState()} of
 *         last consumed {@link CdcManagerRecord}.
 *     </li>
 *     <li>
 *         Apache Ignite provides a default implementation - {@link CdcUtilityActiveCdcManager}. It is disabled from the
 *         beginning, logs the {@link CdcManagerStopRecord} after Ignite node started, and then delegates consuming CDC
 *         events to the {@link CdcMain} utility.
 *     </li>
 * </ul>
 *
 * Apache Ignite can be extended with custom CDC manager. It's required to implement this interface, create own {@link PluginProvider}
 * that will return the implementation with {@link PluginProvider#createComponent(PluginContext, Class)} method.
 * The {@code Class} parameter in the method is {@code CdcManager} class.
 *
 * @see CdcConsumer#onEvents(Iterator)
 * @see CdcConsumerState#saveCdcMode(CdcMode)
 */
@IgniteExperimental
public interface CdcManager extends GridCacheSharedManager {
    /**
     * If this manager isn't enabled then Ignite skips notifying the manager with following methods.
     *
     * @return {@code true} if manager is enabled, otherwise {@code false}.
     */
    public boolean enabled();

    /**
     * Callback is executed only once on Ignite node start when binary memory has fully restored and WAL logging is resumed.
     * It's executed before the first call of {@link #collect(ByteBuffer)}.
     * <p> Implementation suggestions:
     * <ul>
     *     <li>Callback can be used for restoring CDC state on Ignite node start, collecting missed events from WAL segments.</li>
     *     <li>Be aware, this method runs in the Ignite system thread and might get longer the Ignite start procedure.</li>
     *     <li>Ignite node will fail in case the method throws an exception.</li>
     * </ul>
     */
    public default void afterBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr,
        GridCacheDatabaseSharedManager.RestoreBinaryState restoreState) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Callback to collect written WAL records. The provided buffer is a continuous part of WAL segment file.
     * The buffer might contain full content of a segment or only piece of it. There are guarantees:
     * <ul>
     *     <li>This method is invoked sequentially.</li>
     *     <li>Provided {@code dataBuf} is a continuation of the previous one.</li>
     *     <li>{@code dataBuf} contains finite number of completed WAL records. No partially written WAL records are present.</li>
     *     <li>Records can be read from the buffer with {@link RecordSerializer#readRecord(FileInput, WALPointer)}.</li>
     *     <li>{@code dataBuf} must not be changed within this method.</li>
     * </ul>
     *
     * <p> Implementation suggestions:
     * <ul>
     *     <li>
     *         Frequence of calling the method depends on frequence of fsyncing WAL segment.
     *         See {@link IgniteSystemProperties#IGNITE_WAL_SEGMENT_SYNC_TIMEOUT}.
     *     </li>
     *     <li>
     *         It must handle the content of the {@code dataBuf} within the calling thread.
     *         Content of the buffer will not be changed before this method returns.
     *     </li>
     *     <li>It must not block the calling thread and work quickly.</li>
     *     <li>Ignite will ignore any {@link Throwable} throwed from this method.</li>
     * </ul>
     *
     * @param dataBuf Buffer that contains data to collect.
     */
    public void collect(ByteBuffer dataBuf);
}
