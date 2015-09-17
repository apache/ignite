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

package org.apache.ignite.internal.visor.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Task to collect last errors on nodes.
 */
@GridInternal
public class VisorNodeSuppressedErrorsTask extends VisorMultiNodeTask<Map<UUID, Long>,
    Map<UUID, IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>>>,
    IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNodeSuppressedErrorsJob job(Map<UUID, Long> arg) {
        return new VisorNodeSuppressedErrorsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>>>
        reduce0(List<ComputeJobResult> results) {
        Map<UUID, IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>>> taskRes =
            new HashMap<>(results.size());

        for (ComputeJobResult res : results) {
            IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>> jobRes = res.getData();

            taskRes.put(res.getNode().id(), jobRes);
        }

        return taskRes;
    }

    /**
     * Job to collect last errors on nodes.
     */
    private static class VisorNodeSuppressedErrorsJob extends VisorJob<Map<UUID, Long>,
        IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Map with last error counter.
         * @param debug Debug flag.
         */
        private VisorNodeSuppressedErrorsJob(Map<UUID, Long> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Long, List<IgniteExceptionRegistry.ExceptionInfo>> run(Map<UUID, Long> arg) {
            Long lastOrder = arg.get(ignite.localNode().id());

            long order = lastOrder != null ? lastOrder : 0;

            List<IgniteExceptionRegistry.ExceptionInfo> errors = ignite.context().exceptionRegistry().getErrors(order);

            List<IgniteExceptionRegistry.ExceptionInfo> wrapped = new ArrayList<>(errors.size());

            for (IgniteExceptionRegistry.ExceptionInfo error : errors) {
                if (error.order() > order)
                    order = error.order();

                wrapped.add(new IgniteExceptionRegistry.ExceptionInfo(error.order(),
                    new VisorExceptionWrapper(error.error()),
                    error.message(),
                    error.threadId(),
                    error.threadName(),
                    error.time()));
            }

            return new IgniteBiTuple<>(order, wrapped);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodeSuppressedErrorsJob.class, this);
        }
    }
}