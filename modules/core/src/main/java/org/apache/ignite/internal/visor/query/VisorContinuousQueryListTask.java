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

package org.apache.ignite.internal.visor.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** Task to collect Continuous Queries */
@GridInternal
@GridVisorManagementTask
public class VisorContinuousQueryListTask extends VisorMultiNodeTask<VisorContinuousQueryListTaskArg, Collection<VisorContinuousQueryListTaskResult>, Collection<VisorContinuousQueryListTaskResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorContinuousQueryListTaskArg, Collection<VisorContinuousQueryListTaskResult>> job(VisorContinuousQueryListTaskArg arg) {
        return new VisorContinuousQueryListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Collection<VisorContinuousQueryListTaskResult> reduce0(List<ComputeJobResult> list) throws IgniteException {
        Collection<VisorContinuousQueryListTaskResult> ret = new HashSet<>();

        list.forEach(e -> {
            Collection<VisorContinuousQueryListTaskResult> d = e.getData();

            ret.addAll(d);
        });

        return ret;
    }

    /** Job to collect Continuous Queries */
    private static class VisorContinuousQueryListJob extends VisorJob<VisorContinuousQueryListTaskArg, Collection<VisorContinuousQueryListTaskResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        public VisorContinuousQueryListJob(VisorContinuousQueryListTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorContinuousQueryListTaskResult> run(@Nullable VisorContinuousQueryListTaskArg arg) throws IgniteException {
            return
                    ignite.context().continuous().getLocalContinuousQueryRoutines().stream().map(routine -> {
                        GridContinuousHandler hnr = routine.getValue().handler();
                        CacheContinuousQueryHandler cqh = (CacheContinuousQueryHandler) hnr;

                        try {
                            return
                                    new VisorContinuousQueryListTaskResult(
                                            routine.getKey(),
                                            routine.getValue().nodeId(),
                                            cqh.cacheName(),
                                            cqh.localOnly(),
                                            cqh.localListener().getClass().getName(),
                                            cqh.getEventFilter().getClass().getName()
                                    );
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException("P2P unmarshalling failed while retreaving CacheEntryEventFilter", e);
                        }
                    }).collect(Collectors.toList());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorContinuousQueryListJob.class, this);
        }
    }
}
