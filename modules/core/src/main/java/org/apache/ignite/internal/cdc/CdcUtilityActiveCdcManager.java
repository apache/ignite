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

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.cdc.CdcMain.stateDirFile;

/**
 * CDC manager that delegates consuming CDC events to the {@link CdcMain} utility.
 */
public class CdcUtilityActiveCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
    /** If {@code true} then should notify {@link CdcMain} to start consuming WAL segments. */
    private boolean writeStopRecord = true;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        File stateDir = stateDirFile(cctx);

        if (stateDir.exists()) {
            CdcConsumerState state = new CdcConsumerState(log, stateDir.toPath());

            writeStopRecord = state.loadCdcMode() == CdcMode.IGNITE_NODE_ACTIVE;
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart0(boolean active) {
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

    /** {@inheritDoc} */
    @Override public boolean active() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void collect(ByteBuffer dataBuf) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void afterMemoryRestore() throws IgniteCheckedException {
        // No-op.
    }
}
