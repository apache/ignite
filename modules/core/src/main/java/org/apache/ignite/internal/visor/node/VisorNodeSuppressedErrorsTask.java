/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.jetbrains.annotations.Nullable;

/**
 * Task to collect last errors on nodes.
 */
@GridInternal
public class VisorNodeSuppressedErrorsTask extends VisorMultiNodeTask<VisorNodeSuppressedErrorsTaskArg,
    Map<UUID, VisorNodeSuppressedErrors>, VisorNodeSuppressedErrors> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNodeSuppressedErrorsJob job(VisorNodeSuppressedErrorsTaskArg arg) {
        return new VisorNodeSuppressedErrorsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, VisorNodeSuppressedErrors>
        reduce0(List<ComputeJobResult> results) {
        Map<UUID, VisorNodeSuppressedErrors> taskRes =
            new HashMap<>(results.size());

        for (ComputeJobResult res : results) {
            VisorNodeSuppressedErrors jobRes = res.getData();

            taskRes.put(res.getNode().id(), jobRes);
        }

        return taskRes;
    }

    /**
     * Job to collect last errors on nodes.
     */
    private static class VisorNodeSuppressedErrorsJob extends VisorJob<VisorNodeSuppressedErrorsTaskArg, VisorNodeSuppressedErrors> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Map with last error counter.
         * @param debug Debug flag.
         */
        private VisorNodeSuppressedErrorsJob(VisorNodeSuppressedErrorsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorNodeSuppressedErrors run(VisorNodeSuppressedErrorsTaskArg arg) {
            Long lastOrder = arg.getOrders().get(ignite.localNode().id());

            long order = lastOrder != null ? lastOrder : 0;

            List<IgniteExceptionRegistry.ExceptionInfo> errors = ignite.context().exceptionRegistry().getErrors(order);

            List<VisorSuppressedError> wrapped = new ArrayList<>(errors.size());

            for (IgniteExceptionRegistry.ExceptionInfo error : errors) {
                if (error.order() > order)
                    order = error.order();

                wrapped.add(new VisorSuppressedError(error.order(),
                    new VisorExceptionWrapper(error.error()),
                    error.message(),
                    error.threadId(),
                    error.threadName(),
                    error.time()));
            }

            return new VisorNodeSuppressedErrors(order, wrapped);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodeSuppressedErrorsJob.class, this);
        }
    }
}
