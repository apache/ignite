/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.visor.misc;

import java.io.File;
import java.io.FileFilter;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Performs WAL cleanup clusterwide.
 */
@GridInternal
public class VisorWalTask extends VisorMultiNodeTask<VisorWalTaskArg, VisorWalTaskResult, Collection<String>> {
    /** Pattern for segment file names */
    private static final Pattern WAL_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal");

    /** */
    private static final Pattern WAL_SEGMENT_FILE_COMPACTED_PATTERN = Pattern.compile("\\d{16}\\.wal\\.zip");

    /** */
    private static final FileFilter WAL_ARCHIVE_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return  !file.isDirectory() && (WAL_NAME_PATTERN.matcher(file.getName()).matches() ||
                    WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches());
        }
    };


    /** {@inheritDoc} */
    @Override protected VisorWalJob job(VisorWalTaskArg arg) {
        return new VisorWalJob(arg, debug);
    }


    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorWalTaskArg> arg) {
        Collection<ClusterNode> srvNodes = ignite.cluster().forServers().nodes();
        Collection<UUID> ret = new ArrayList<>(srvNodes.size());

        VisorWalTaskArg taskArg = arg.getArgument();

        Set<String> nodeIds = taskArg.getConsistentIds() != null ? new HashSet<>(arg.getArgument().getConsistentIds())
                                : null;

        for(ClusterNode node: srvNodes) {
            if(nodeIds == null || nodeIds.contains(node.consistentId().toString()))
                ret.add(node.id());
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorWalTaskResult reduce0(List<ComputeJobResult> results)
            throws IgniteException {
        Map<String, Exception> exRes = U.newHashMap(0);
        Map<String, Collection<String>> res = U.newHashMap(results.size());
        Map<String, VisorClusterNode> nodesInfo = U.newHashMap(results.size());

        for (ComputeJobResult result: results){
            ClusterNode node = result.getNode();

            String nodeId = node.consistentId().toString();

            if(result.getException() != null)
                exRes.put(nodeId, result.getException());
            else if (result.getData() != null) {
                Collection<String> data = result.getData();

                if(data != null)
                    res.put(nodeId, data);
            }

            nodesInfo.put(nodeId, new VisorClusterNode(node));
        }

        return new VisorWalTaskResult(res, exRes, nodesInfo);
    }

    /**
     * Performs WAL cleanup per node.
     */
    private static class VisorWalJob extends VisorJob<VisorWalTaskArg, Collection<String>> {
        /** Auto injected logger */
        @LoggerResource
        private transient IgniteLogger log;

        /** */
        private static final long serialVersionUID = 0L;

        private transient IgniteConfiguration igCfg;

        private transient DataStorageConfiguration dsCfg;

        private transient GridKernalContext cctx;

        private transient File walArchiveDir;

        private transient GridCacheDatabaseSharedManager dbMgr;

        /**
         *  @param arg WAL task argument.
         *  @param debug Debug flag.
         */
        public VisorWalJob(VisorWalTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * Initialize {@link VisorWalJob}
         */
        private void init() throws IgniteCheckedException {
            cctx = ignite.context();

            igCfg = cctx.config();

            dsCfg = igCfg.getDataStorageConfiguration();

            walArchiveDir = getWalArchiveDir();

            dbMgr = (GridCacheDatabaseSharedManager) cctx.cache().context().database();
        }


        /** {@inheritDoc} */
        @Nullable @Override protected Collection<String> run(@Nullable VisorWalTaskArg arg) throws IgniteException {
            try {
                init();

                if(dsCfg == null || dbMgr == null || walArchiveDir == null  || arg == null)
                    return null;

                switch (arg.getOperation()){
                    case DELETE_UNUSED_WAL_SEGMENTS:
                        return deleteUnusedWalSegments(dbMgr);

                    case PRINT_UNUSED_WAL_SEGMENTS:
                    default:
                        return getUnusedWalSegments(dbMgr);

                }
            }
            catch (IgniteCheckedException e){
                U.error(log, "Failed to perform WAL task", e);

                throw new IgniteException("Failed to perform WAL task",e);
            }
        }

        /**
         * Get unused wal segments.
         *
         * @param  dbMgr Database manager.
         * @return {@link Collection<String>} of absolute paths of unused WAL segments
         * @throws IgniteCheckedException if failed.
         */
        Collection<String> getUnusedWalSegments(GridCacheDatabaseSharedManager dbMgr) throws IgniteCheckedException {
            long maxIdx = dbMgr.reservedWalSegmentIndex();

            File [] walFiles = getWalArchiveDir().listFiles(WAL_ARCHIVE_FILE_FILTER);

            Collection<String> res = new ArrayList<>(walFiles != null && walFiles.length > 0 ? walFiles.length - 1 : 0);

            if(walFiles != null)
                for(File f: walFiles) {
                    if (getIndex(f) < maxIdx && res.size() < walFiles.length - 1)
                        res.add(f.getAbsolutePath());
                    else
                        break;
                }

            return res;
        }

        /**
         * Delete unused wal segments.
         *
         * @param dbMgr Database manager.
         * @return {@link Collection<String>} deleted WAL segment's files.
         */
        Collection<String> deleteUnusedWalSegments(GridCacheDatabaseSharedManager dbMgr) {
            long maxIdx = dbMgr.reservedWalSegmentIndex();

            File[] walFiles = walArchiveDir.listFiles(WAL_ARCHIVE_FILE_FILTER);

            int num = dbMgr.walSegmentsTruncate();

            if (walFiles != null) {
                Arrays.sort(walFiles, new Comparator<File>() {
                    @Override public int compare(File o1, File o2) {
                        return Long.compare(getIndex(o1), getIndex(o2));
                    }
                });

                Collection<String> res = new ArrayList<>(num);

                for (File walFile: walFiles) {
                    if (getIndex(walFile) < maxIdx && num >= 0 && res.size() < walFiles.length - 1)
                        res.add(walFile.getAbsolutePath());
                    else
                        break;

                    num--;
                }

                return res;
            }
            else
                return Collections.emptyList();

        }

        /**
         * Get WAL archive directory from configuration.
         *
         * @return WAL archive directory.
         * @throws IgniteCheckedException if failed.
         */
        private File getWalArchiveDir() throws IgniteCheckedException {
            PdsFolderSettings resFldrs = cctx.pdsFolderResolver().resolveFolders();

            String consId = resFldrs.folderName();

            File dir;

            if (dsCfg.getWalArchivePath() != null) {
                File workDir0 = new File(dsCfg.getWalArchivePath());

                dir = workDir0.isAbsolute() ?
                        new File(workDir0, consId) :
                        new File(U.resolveWorkDirectory(igCfg.getWorkDirectory(), dsCfg.getWalArchivePath(), false),
                                consId);
            } else
                dir = new File(U.resolveWorkDirectory(igCfg.getWorkDirectory(),
                        DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH, false), consId);

            U.ensureDirectory(dir, "WAL archive directory", log);

            return dir;
        }
    }

    /**
     * Get index from wal segment file.
     *
     * @param file File.
     * @return Index of wal segment file.
     */
    private static long getIndex(File file){
        return Long.parseLong(file.getName().substring(0, 16));
    }
}
