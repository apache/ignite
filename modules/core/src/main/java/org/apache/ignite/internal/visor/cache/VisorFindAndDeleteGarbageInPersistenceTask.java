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

package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task which are part of 'find and delete garbage' functionality.
 */
@GridInternal
public class VisorFindAndDeleteGarbageInPersistenceTask extends VisorMultiNodeTask<VisorFindAndDeleteGarbageInPersistenceTaskArg,
    VisorFindAndDeleteGarbageInPersistenceTaskResult, VisorFindAndDeleteGarbageInPersistenceJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected VisorFindAndDeleteGarbageInPersistenceTaskResult reduce0(List<ComputeJobResult> list) throws IgniteException {
        Map<UUID, Exception> exceptions = new HashMap<>();
        Map<UUID, VisorFindAndDeleteGarbageInPersistenceJobResult> jobResults = new HashMap<>();

        for (ComputeJobResult res : list) {
            if (res.getException() != null)
                exceptions.put(res.getNode().id(), res.getException());
            else
                jobResults.put(res.getNode().id(), res.getData());
        }

        return new VisorFindAndDeleteGarbageInPersistenceTaskResult(jobResults, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorFindAndDeleteGarbageInPersistenceTaskArg, VisorFindAndDeleteGarbageInPersistenceJobResult> job(VisorFindAndDeleteGarbageInPersistenceTaskArg arg) {
        return new FindAndDeleteGarbageInPersistenceJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorFindAndDeleteGarbageInPersistenceTaskArg> arg) {
        Collection<ClusterNode> srvNodes = ignite.cluster().forServers().nodes();
        Collection<UUID> ret = new ArrayList<>(srvNodes.size());

        Set<UUID> nodeIds = arg.getArgument().getNodes();

        if (nodeIds == null) {
            for (ClusterNode node : srvNodes)
                ret.add(node.id());
        }
        else {
            for (ClusterNode node : srvNodes) {
                if (nodeIds.contains(node.id()))
                    ret.add(node.id());
            }
        }

        return ret;
    }

    /**
     *
     */
    private static class FindAndDeleteGarbageInPersistenceJob extends VisorJob<VisorFindAndDeleteGarbageInPersistenceTaskArg, VisorFindAndDeleteGarbageInPersistenceJobResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected FindAndDeleteGarbageInPersistenceJob(@Nullable VisorFindAndDeleteGarbageInPersistenceTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorFindAndDeleteGarbageInPersistenceJobResult run(@Nullable VisorFindAndDeleteGarbageInPersistenceTaskArg arg) throws IgniteException {
            try {
                VisorFindAndDeleteGarbageInPersistenceClosure closure = new VisorFindAndDeleteGarbageInPersistenceClosure(arg.getGrpNames(), arg.deleteFoundGarbage());

                ignite.context().resource().injectGeneric(closure);

                return closure.call();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FindAndDeleteGarbageInPersistenceJob.class, this);
        }
    }
}
