/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cdc.CdcConsumerState;
import org.apache.ignite.internal.cdc.CdcFileLockHolder;
import org.apache.ignite.internal.cdc.CdcMode;
import org.apache.ignite.internal.cdc.CdcUtilityActiveCdcManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/** Task to clear CDC directory. */
@GridInternal
public class CdcClearTask extends VisorMultiNodeTask<CdcClearCommandArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CdcClearCommandArg, Void> job(CdcClearCommandArg arg) {
        return new CdcClearJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Void reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                throw new IgniteException("Failed to clear CDC directory on a node " +
                    "[nodeId=" + res.getNode().id() + "]. The command can be rerun after fixing the error.",
                    res.getException());
            }
        }

        return null;
    }

    /** */
    private static class CdcClearJob extends VisorJob<CdcClearCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @LoggerResource
        protected IgniteLogger log;

        /** */
        protected CdcClearJob(CdcClearCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(CdcClearCommandArg arg) throws IgniteException {
            if (!CU.isCdcEnabled(ignite.configuration()))
                throw new IgniteException("CDC is not configured.");

            NodeFileTree ft = ignite.context().pdsFolderResolver().fileTree();

            CdcFileLockHolder lock = new CdcFileLockHolder(ft.walCdc().getAbsolutePath(), "Clear CDC job", log);

            try {
                lock.tryLock(1);

                // Links go first, in ascending order: on partial failure the remaining links follow the kept WAL state,
                // so the CDC application fails on start instead of consuming them.
                File[] links = ft.walCdcSegments();

                Arrays.sort(links, Comparator.comparingLong(link -> ft.walSegmentIndex(link.toPath())));

                for (File link : links)
                    Files.delete(link.toPath());

                if (arg.resetMode()) {
                    if (Files.exists(ft.cdcState()) && !U.delete(ft.cdcState()))
                        throw new IgniteException("Failed to delete CDC state directory [dir=" + ft.cdcState() + ']');
                }
                else {
                    deleteStateExceptMode(ft);
                    saveUtilityActiveModeIfAbsent(ft);
                }

                log.info("CDC directory cleared [dir=" + ft.walCdc().getAbsolutePath() +
                    ", deletedLinksCnt=" + links.length + ']');

                return null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to clear CDC directory. " +
                    "Unable to acquire lock to the CDC folder. Make sure a CDC app is shut down " +
                    "[dir=" + ft.walCdc().getAbsolutePath() + ", reason=" + e.getMessage() + ']', e);
            }
            catch (IOException e) {
                throw new IgniteException("Failed to clear CDC directory " +
                    "[dir=" + ft.walCdc().getAbsolutePath() + ']', e);
            }
            finally {
                U.closeQuiet(lock);
            }
        }

        /**
         * Deletes CDC application state except the mode, otherwise the application waits for
         * {@code CdcManagerStopRecord}, which is not logged again.
         *
         * @param ft Node file tree.
         */
        private void deleteStateExceptMode(NodeFileTree ft) throws IOException {
            // Including temporary files left after a failed save.
            List<Path> stateFiles = List.of(
                ft.cdcWalState(), ft.tmpCdcWalState(),
                ft.cdcCachesState(), ft.tmpCdcCachesState(),
                ft.cdcMappingsState(), ft.tmpCdcMappingsState(),
                ft.cdcTypesState(), ft.tmpCdcTypesState(),
                ft.tmpCdcModeState());

            for (Path file : stateFiles)
                Files.deleteIfExists(file);
        }

        /**
         * Saves {@link CdcMode#CDC_UTILITY_ACTIVE} for the default CDC manager if the mode is absent:
         * {@code CdcManagerStopRecord} is logged once on node start and may be in a deleted link.
         *
         * @param ft Node file tree.
         */
        private void saveUtilityActiveModeIfAbsent(NodeFileTree ft) throws IOException {
            boolean dfltMgr = ignite.context().cache().context().cdc() instanceof CdcUtilityActiveCdcManager;

            if (!dfltMgr || Files.exists(ft.cdcModeState()))
                return;

            // Absent if the CDC application has never started.
            Files.createDirectories(ft.cdcState());

            new CdcConsumerState(log, ft).saveCdcMode(CdcMode.CDC_UTILITY_ACTIVE);
        }
    }
}
