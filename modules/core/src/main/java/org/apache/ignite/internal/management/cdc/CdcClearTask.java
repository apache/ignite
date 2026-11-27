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

package org.apache.ignite.internal.management.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cdc.CdcFileLockHolder;
import org.apache.ignite.internal.cdc.CdcManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/** Task to clear CDC directory. */
@GridInternal
public class CdcClearTask extends VisorMultiNodeTask<CdcClearCommandArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final String FEATURE_NOT_ACTIVE_ERR_MSG = "Won't clear CDC directory. " +
        "The CDC clear feature isn't activated yet. The Rolling Upgrade has to be finished.";

    /** {@inheritDoc} */
    @Override protected VisorJob<CdcClearCommandArg, Void> job(CdcClearCommandArg arg) {
        return new CdcClearJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<CdcClearCommandArg> arg) {
        if (!ignite.context().rollingUpgrade().features().isActive(CoreFeatureRegistry.CDC_CLEAR_FEATURE))
            throw new IgniteException(FEATURE_NOT_ACTIVE_ERR_MSG);

        return super.jobNodes(arg);
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
            try {
                clear(arg);

                return null;
            }
            catch (Throwable e) {
                // Compute logs job failures only in debug mode, and the task reports the error of a single node.
                U.error(log, "Failed to clear CDC directory [resetMode=" + arg.resetMode() + ']', e);

                throw e;
            }
        }

        /**
         * Clears the CDC directory of the local node.
         *
         * @param arg Command argument.
         */
        private void clear(CdcClearCommandArg arg) {
            if (!CU.isCdcEnabled(ignite.configuration()))
                throw new IgniteException("CDC is not configured.");

            NodeFileTree ft = ignite.context().pdsFolderResolver().fileTree();

            if (!ft.walCdc().isDirectory()) {
                throw new IgniteException("Failed to clear CDC directory. The directory does not exist " +
                    "[dir=" + ft.walCdc().getAbsolutePath() + ']');
            }

            CdcManager cdcMgr = ignite.context().cache().context().cdc();

            CdcFileLockHolder lock = lock(ft);

            try {
                try {
                    cdcMgr.beforeClear();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to clear CDC directory. CDC manager failed to prepare for clearing " +
                        "[cdcManager=" + cdcMgr.getClass().getName() + ", reason=" + e.getMessage() + ']', e);
                }

                // Order matters on partial failure: links go first in ascending order, so the kept links remain a contiguous
                // tail, then the state with the WAL state last and the mode after it. So if the WAL state is kept, the CDC
                // application either resumes from it or fails on start, but never silently skips events, types, mappings or caches.
                File[] links = ft.walCdcSegments();

                if (links == null)
                    throw new IOException("Failed to list WAL segment links");

                Arrays.sort(links, Comparator.comparingLong(link -> ft.walSegmentIndex(link.toPath())));

                int deletedLinksCnt = 0;

                for (File link : links) {
                    if (Files.deleteIfExists(link.toPath()))
                        deletedLinksCnt++;
                }

                deleteStateExceptMode(ft);

                if (arg.resetMode()) {
                    Files.deleteIfExists(ft.cdcModeState());

                    if (Files.exists(ft.cdcState()) && !U.delete(ft.cdcState()))
                        throw new IgniteException("Failed to delete CDC state directory [dir=" + ft.cdcState() + ']');
                }

                cdcMgr.afterClear(arg.resetMode());

                log.info("CDC directory cleared [dir=" + ft.walCdc().getAbsolutePath() +
                    ", deletedLinksCnt=" + deletedLinksCnt + ']');
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException("Failed to clear CDC directory " +
                    "[dir=" + ft.walCdc().getAbsolutePath() + ']', e);
            }
            finally {
                U.closeQuiet(lock);
            }
        }

        /** Acquires lock on the CDC directory. */
        private CdcFileLockHolder lock(NodeFileTree ft) {
            CdcFileLockHolder lock = new CdcFileLockHolder(ft.walCdc().getAbsolutePath(), "Clear CDC job", log);

            try {
                lock.tryLock(1);

                return lock;
            }
            catch (IgniteCheckedException e) {
                U.closeQuiet(lock);

                throw new IgniteException("Failed to clear CDC directory. " +
                    "Unable to acquire lock to the CDC folder. Make sure a CDC app is shut down " +
                    "and no other CDC command is running on the node " +
                    "[dir=" + ft.walCdc().getAbsolutePath() + ", reason=" + e.getMessage() + ']', e);
            }
        }

        /**
         * Deletes CDC application state except the mode.
         *
         * @param ft Node file tree.
         */
        private void deleteStateExceptMode(NodeFileTree ft) throws IOException {
            // Including temporary files left after a failed save. The WAL state goes last.
            List<Path> stateFiles = List.of(
                ft.cdcCachesState(), ft.tmpCdcCachesState(),
                ft.cdcMappingsState(), ft.tmpCdcMappingsState(),
                ft.cdcTypesState(), ft.tmpCdcTypesState(),
                ft.tmpCdcModeState(),
                ft.tmpCdcWalState(), ft.cdcWalState());

            for (Path file : stateFiles)
                Files.deleteIfExists(file);
        }
    }
}
