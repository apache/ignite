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

package org.apache.ignite.internal.management.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Enabled events status task.
 */
@GridInternal
public class EventStatusTask extends VisorMultiNodeTask<NoArg, Collection<String>, Collection<String>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, Collection<String>> job(NoArg arg) {
        return new EventStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Collection<String> reduce0(List<ComputeJobResult> results) {
        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();
        }

        Set<String> res = new TreeSet<>((Collection<String>)results.get(0).getData());

        for (int i = 1; i < results.size(); i++) {
            Collection<String> res0 = results.get(i).getData();

            res.retainAll(res0);
        }

        for (int i = 0; i < results.size(); i++) {
            Collection<String> res0 = results.get(i).getData();

            for (String evtName : res0) {
                if (!res.contains(evtName))
                    res.add("Warning: Event " + evtName + " is enabled only on part of nodes");
            }
        }

        return res;
    }

    /** The job for view enabled events status. */
    private static class EventStatusJob extends VisorJob<NoArg, Collection<String>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected EventStatusJob(NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<String> run(NoArg arg) throws IgniteException {
            int[] evts = ignite.context().event().enabledEvents();

            Collection<String> res = new ArrayList<>();

            for (int i = 0; i < evts.length; i++)
                res.add("EVT_" + U.gridEventName(evts[i]));

            return res;
        }
    }
}
