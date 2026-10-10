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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.lang.String.format;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.CDC_DISABLED;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.util.io.GridFileUtils.ensureHardLinkAvailable;

/**
 * Links the archived WAL segments to the CDC directory. The links are not created while CDC is disabled by the
 * {@link FileWriteAheadLogManager#CDC_DISABLED} distributed property. Data regions without persistence do not log the
 * changes while CDC is disabled, so on the nodes with such regions the segment that is current at the disabling is never
 * linked either: the CDC application fails on the missed segment after CDC is enabled again.
 */
class CdcLinkManager {
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final NodeFileTree ft;

    /** CDC disabled flag. */
    private final DistributedBooleanProperty cdcDisabled = detachedBooleanProperty(CDC_DISABLED,
        "CDC disabled flag. Disables CDC in the cluster to avoid disk overflow. " +
            "Note that cache changes will be lost when CDC is disabled. Useful if the CDC application " +
            "is down for a long time.");

    /**
     * Index of the segment that was current at the last CDC disabling. It is never linked, so the CDC application detects
     * the missed changes as a gap in the segments even if no segment is archived while CDC is disabled. Persistent
     * regions log the changes anyway, so the gap is made only on the nodes with in-memory CDC regions.
     */
    private volatile long disableSgmnt = -1;

    /** */
    CdcLinkManager(GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        this.cctx = cctx;

        log = cctx.logger(CdcLinkManager.class);
        ft = cctx.kernalContext().pdsFolderResolver().fileTree();

        DataStorageConfiguration dsCfg = cctx.gridConfig().getDataStorageConfiguration();

        // True on a node with persistence too, unlike FileWriteAheadLogManager#inMemoryCdc.
        boolean inMemoryCdcRegion = F.exist(
            F.concat(false, dsCfg.getDefaultDataRegionConfiguration(), F.asList(dsCfg.getDataRegionConfigurations())),
            reg -> reg.isCdcEnabled() && !reg.isPersistenceEnabled());

        U.ensureDirectory(ft.walCdc(), "change data capture directory", log);

        ensureHardLinkAvailable(ft.walArchive().toPath(), ft.walCdc().toPath());

        cctx.kernalContext().internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            cdcDisabled.addListener((name, oldVal, newVal) -> {
                if (log.isInfoEnabled())
                    log.info(format("Distributed property '%s' was changed from '%s' to '%s'.", name, oldVal, newVal));

                if (newVal != null && newVal) {
                    log.warning("CDC was disabled.");

                    if (cctx.cdc() != null)
                        cctx.cdc().stop(true);

                    if (inMemoryCdcRegion)
                        markDisableSegment();
                }
            });

            dispatcher.registerProperty(cdcDisabled);
        });
    }

    /** */
    private void markDisableSegment() {
        // -1 if the WAL logging is not resumed yet: the node is restarted with CDC disabled, the gap is already made.
        disableSgmnt = Math.max(disableSgmnt, cctx.wal(true).currentSegment());
    }

    /** */
    boolean forceDisabled() {
        return cdcDisabled.getOrDefault(false);
    }

    /**
     * The segment that was current at the CDC disabling is never linked, the changes in it are lost. It is closed before
     * the next record, so the changes made after CDC is enabled again do not get into it.
     */
    boolean shouldRollover(long idx) {
        return idx == disableSgmnt;
    }

    /** */
    void linkSegment(long idx, File segment) throws IOException {
        if (forceDisabled())
            log.warning("Creation of segment CDC link skipped. '" + CDC_DISABLED + "' distributed property is 'true'.");
        else if (idx <= disableSgmnt)
            log.warning("Creation of segment CDC link skipped. The segment was not archived before CDC was disabled.");
        else if (!checkCdcWalDirectorySize(segment.length()))
            log.error("Creation of segment CDC link skipped. Configured CDC directory maximum size exceeded.");
        else
            Files.createLink(ft.walCdc().toPath().resolve(segment.getName()), segment.toPath());
    }

    /**
     * @param len Length of file to check size.
     * @return {@code True} if the CDC directory size check successful, otherwise {@code false}.
     */
    private boolean checkCdcWalDirectorySize(long len) {
        long maxDirSize = cctx.gridConfig().getDataStorageConfiguration().getCdcWalDirectoryMaxSize();

        if (maxDirSize <= 0)
            return true;

        long dirSize = Arrays.stream(ft.walCdcSegments()).mapToLong(File::length).sum();

        if (dirSize + len <= maxDirSize)
            return true;

        log.warning("Configured CDC WAL directory maximum size exceeded [curDirSize=" + dirSize +
            ", fileLength=" + len + ", cdcWalDirectoryMaxSize=" + maxDirSize + ']');

        return false;
    }
}
