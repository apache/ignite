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

import org.apache.ignite.compute.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

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

            for (IgniteExceptionRegistry.ExceptionInfo error : errors) {
                if (error.order() > order)
                    order = error.order();
            }

            return new IgniteBiTuple<>(order, errors);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodeSuppressedErrorsJob.class, this);
        }
    }
}
