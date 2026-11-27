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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.CdcManagerStopRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * CDC manager that delegates consuming CDC events to the {@link CdcMain} utility.
 */
public class CdcUtilityActiveCdcManager extends GridCacheSharedManagerAdapter implements CdcManager, PartitionsExchangeAware {
    /** */
    @Override protected void start0() {
        cctx.exchange().registerExchangeAwareComponent(this);
    }

    /** */
    @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.localJoinExchange() || fut.activateCluster()) {
            try {
                File cdcModeFile = cctx.kernalContext().pdsFolderResolver().fileTree().cdcModeState().toAbsolutePath().toFile();

                if (!cdcModeFile.exists())
                    cctx.wal(true).log(new CdcManagerStopRecord());
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to activate CdcManager. CDC might not work.");
            }
            finally {
                cctx.exchange().unregisterExchangeAwareComponent(this);
            }
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
    @Override public void beforeClear() {
        // No-op. Events are consumed by CdcMain, whose state is cleared.
    }

    /** {@inheritDoc} */
    @Override public void afterClear(boolean resetMode) throws IgniteCheckedException {
        // CdcMain switches to the mode after node restart.
        if (resetMode)
            return;

        // CdcManagerStopRecord might be in a deleted segment link and is not logged again: it is logged once after node start.
        // The mode is overwritten if it exists, because events are consumed by CdcMain with this manager.
        NodeFileTree ft = cctx.kernalContext().pdsFolderResolver().fileTree();

        try {
            // Absent if CdcMain has never started.
            Files.createDirectories(ft.cdcState());

            new CdcConsumerState(log, ft).saveCdcMode(CdcMode.CDC_UTILITY_ACTIVE);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to save CDC mode [mode=" + CdcMode.CDC_UTILITY_ACTIVE + ']', e);
        }
    }
}
