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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cdc.CdcMain.STATE_DIR;

/**
 * CDC is based on consuming by {@link CdcMain} WAL segments stored in {@link DataStorageConfiguration#getCdcWalPath()}.
 */
public class FileCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
    /** If {@code true} then should notify {@link CdcMain} to start consuming WAL segments. */
    private boolean writeStopRecord;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        try {
            Path stateDir = ((FileWriteAheadLogManager)cctx.wal(true)).walCdcDirectory()
                .toPath().resolve(STATE_DIR);

            Files.createDirectories(stateDir);

            CdcConsumerState state = new CdcConsumerState(log, stateDir);

            state.saveCdcManagerMode(CdcManagerMode.CDC_UTILITY_ACTIVE);

            writeStopRecord = true;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void collect(ByteBuffer dataBuf, int off, int limit) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void beforeResumeLogging(@Nullable WALPointer ptr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void afterResumeLogging() {
        try {
            if (writeStopRecord) {
                cctx.wal(true).log(new CdcManagerStopRecord());

                writeStopRecord = false;
            }
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to write WAL record. CDC might not work.");
        }
    }
}
