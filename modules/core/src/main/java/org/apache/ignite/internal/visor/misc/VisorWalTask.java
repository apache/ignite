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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;

/**
 *
 */
@GridInternal
public class VisorWalTask extends VisorMultiNodeTask<VisorWalTaskArg, VisorWalTaskResult, Collection<String>> {
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
     *  Job to retrive list of unused wal segments files.
     */
    private static class VisorWalJob extends VisorJob<VisorWalTaskArg, Collection<String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *  @param debug Debug flag.
         */
        public VisorWalJob(VisorWalTaskArg arg, boolean debug) {
            super(arg, debug);
        }


        /** {@inheritDoc} */
        @Nullable @Override protected Collection<String> run(@Nullable VisorWalTaskArg arg) throws IgniteException {
            GridCacheSharedContext<Object, Object> ctx = ignite.context().cache().context();
            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) ctx.database();

            if(dbMgr == null || arg == null)
                return null;

            try {
                switch (arg.getOperation()){
                    case DELETE_UNUSED_WAL_SEGMENTS:
                        deleteUnusedWalSegments(dbMgr);
                        return Collections.emptyList();

                    case PRINT_UNUSED_WAL_SEGMENTS:
                    default:
                        return getUnusedWalSegments(dbMgr);

                }
            } catch (IgniteCheckedException e){
                throw new IgniteException("Obtained unused wal files list failed",e);
            }
        }

        /**
         *  Return unused wal segments.
         *
         * @param dbMgr Dg manager.
         * @return Collection of absolute paths of unused WAL segments
         * @throws IgniteCheckedException if failed.
         */
        Collection<String> getUnusedWalSegments(GridCacheDatabaseSharedManager dbMgr) throws IgniteCheckedException {
            File[] walFiles = dbMgr.walArchiveCanBeTruncated();
            Collection<String> res = new ArrayList<>(walFiles.length);

            for(File f: walFiles)
                res.add(f.getAbsolutePath());

            return res;
        }

        /**
         *  Delete unused wal segments.
         *
         * @param dbMgr Dg manager.
         * @return Number of deleted WAL segments.
         * @throws IgniteCheckedException if failed.
         */
        int deleteUnusedWalSegments(GridCacheDatabaseSharedManager dbMgr) throws IgniteCheckedException {
            File[] walFiles = dbMgr.walArchiveCanBeTruncated();
            Collection<String> res = new ArrayList<>(walFiles != null ? walFiles.length : 0);
            int num = dbMgr.walArchiveTruncate();
            ignite.log().info(String.format("WAL archive was truncated, %d segments deleted", num));
            return num;
        }
    }
}
