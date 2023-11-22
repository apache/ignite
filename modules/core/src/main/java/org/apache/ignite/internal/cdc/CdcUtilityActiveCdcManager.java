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
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.cdc.CdcMain.STATE_DIR;

/**
 * CDC manager that delegates consuming CDC events to the {@link CdcMain} utility.
 */
public class CdcUtilityActiveCdcManager extends GridCacheSharedManagerAdapter implements CdcManager {
    /** {@inheritDoc} */
    @Override public void onActivate() {
        try {
            File stateDir = new File(((FileWriteAheadLogManager)cctx.wal(true)).walCdcDirectory(), STATE_DIR);

            boolean logStopRecord = true;

            if (stateDir.exists()) {
                CdcConsumerState state = new CdcConsumerState(log, stateDir.toPath());

                logStopRecord = state.loadCdcMode() == CdcMode.IGNITE_NODE_ACTIVE;
            }

            if (logStopRecord)
                cctx.wal(true).log(new CdcManagerStopRecord());
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to write WAL record. CDC might not work.");
        }
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void collect(ByteBuffer dataBuf) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void afterMemoryRestore() {
        // No-op.
    }
}
