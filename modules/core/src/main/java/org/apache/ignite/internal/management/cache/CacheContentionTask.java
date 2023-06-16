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

package org.apache.ignite.internal.management.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridInternal
public class CacheContentionTask extends VisorMultiNodeTask<CacheContentionCommandArg,
    CacheContentionTaskResult, CacheContentionJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected CacheContentionTaskResult reduce0(List<ComputeJobResult> list) throws IgniteException {
        Map<UUID, Exception> exceptions = new HashMap<>();
        List<CacheContentionJobResult> infos = new ArrayList<>();

        for (ComputeJobResult res : list) {
            if (res.getException() != null)
                exceptions.put(res.getNode().id(), res.getException());
            else
                infos.add(res.getData());
        }

        return new CacheContentionTaskResult(infos, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<CacheContentionCommandArg, CacheContentionJobResult> job(CacheContentionCommandArg arg) {
        return new CacheContentionJob(arg, debug);
    }

    /**
     *
     */
    private static class CacheContentionJob extends VisorJob<CacheContentionCommandArg, CacheContentionJobResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected CacheContentionJob(@Nullable CacheContentionCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected CacheContentionJobResult run(@Nullable CacheContentionCommandArg arg) throws IgniteException {
            try {
                ContentionClosure clo = new ContentionClosure(arg.minQueueSize(), arg.maxPrint());

                ignite.context().resource().injectGeneric(clo);

                ContentionInfo info = clo.call();

                return new CacheContentionJobResult(info);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheContentionJob.class, this);
        }
    }
}
