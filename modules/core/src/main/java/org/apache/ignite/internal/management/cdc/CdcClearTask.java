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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cdc.CdcFileLockHolder;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.CDC_DISABLED;

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
            GridKernalContext ctx = ignite.context();

            if (!CU.isCdcEnabled(ignite.configuration()))
                throw new IgniteException("CDC is not configured.");

            if (!ctx.distributedConfiguration().<Boolean>property(CDC_DISABLED).getOrDefault(false))
                throw new IgniteException("CDC was not disabled.");

            NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

            CdcFileLockHolder lock = new CdcFileLockHolder(ft.walCdc().getAbsolutePath(), "Clear CDC job", log);

            try {
                lock.tryLock(1);

                // Temporary state files can remain if the CDC application failed while saving its state.
                Stream<Path> statePaths = Stream.of(
                    ft.cdcWalState(), ft.tmpCdcWalState(),
                    ft.cdcCachesState(), ft.tmpCdcCachesState(),
                    ft.cdcMappingsState(), ft.tmpCdcMappingsState(),
                    ft.cdcTypesState(), ft.tmpCdcTypesState(),
                    ft.tmpCdcModeState());

                // Mode state is kept by default: without it the CDC application waits for CdcManagerStopRecord, which a
                // running node does not log again (the default CdcManager logs it only once after node start, a custom
                // one once on stop). So if mode state is cleared, changes would not be captured by CDC application
                // until node restart with enabled CDC: segments archived while CDC is disabled are not linked.
                if (arg.resetMode())
                    statePaths = Stream.concat(statePaths, Stream.of(ft.cdcModeState()));

                List<File> stateFiles = statePaths.map(Path::toFile)
                    .filter(File::exists)
                    .collect(Collectors.toList());

                // State is deleted first: if deletion of segment links fails, the CDC application will not continue
                // from the saved state.
                delete(stateFiles);

                // With mode reset the state directory is expected to be empty, otherwise it contains the mode state.
                if (arg.resetMode()) {
                    File cdcStateDir = ft.cdcState().toFile();

                    if (cdcStateDir.exists() && !cdcStateDir.delete()) {
                        throw new IgniteException("Failed to delete state directory [cdcStateDir=" +
                            cdcStateDir.getAbsolutePath() + "]. Check it is empty or your access rights.");
                    }
                }

                List<File> segments = Arrays.asList(ft.walCdcSegments());

                delete(segments);

                log.info("CDC directory cleared [dir=" + ft.walCdc().getAbsolutePath() +
                    ", deletedStateFilesCnt=" + stateFiles.size() + ", deletedSegmentLinksCnt=" + segments.size() + ']');

                return null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to clear CDC directory. " +
                    "Unable to acquire lock to the CDC folder. Make sure a CDC app is shut down " +
                    "[dir=" + ft.walCdc().getAbsolutePath() + ", reason=" + e.getMessage() + ']', e);
            }
            finally {
                U.closeQuiet(lock);
            }
        }

        /** @param files Files to delete. */
        private static void delete(List<File> files) {
            for (File file : files) {
                if (!file.delete())
                    throw new IgniteException("Failed to delete CDC file [file=" + file.getAbsolutePath() + ']');
            }
        }
    }
}
