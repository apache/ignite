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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.util.VisorEventMapper;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.EVT_MAPPER;

/**
 * Task that runs on specified node and returns events data.
 */
@GridInternal
public class VisorNodeEventsCollectorTask extends VisorMultiNodeTask<VisorNodeEventsCollectorTaskArg,
    Iterable<? extends VisorGridEvent>, Collection<? extends VisorGridEvent>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNodeEventsCollectorJob job(VisorNodeEventsCollectorTaskArg arg) {
        return new VisorNodeEventsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Iterable<? extends VisorGridEvent> reduce0(List<ComputeJobResult> results) {
        Collection<VisorGridEvent> allEvts = new ArrayList<>();

        for (ComputeJobResult r : results) {
            if (r.getException() == null)
                allEvts.addAll((Collection<VisorGridEvent>)r.getData());
        }

        return allEvts.isEmpty() ? Collections.<VisorGridEvent>emptyList() : allEvts;
    }

    /**
     * Job for task returns events data.
     */
    protected static class VisorNodeEventsCollectorJob extends VisorJob<VisorNodeEventsCollectorTaskArg,
        Collection<? extends VisorGridEvent>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected VisorNodeEventsCollectorJob(VisorNodeEventsCollectorTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * Tests whether or not this task has specified substring in its name.
         *
         * @param taskName Task name to check.
         * @param taskClsName Task class name to check.
         * @param s Substring to check.
         */
        private boolean containsInTaskName(String taskName, String taskClsName, String s) {
            assert taskName != null;
            assert taskClsName != null;

            if (taskName.equals(taskClsName)) {
                int idx = taskName.lastIndexOf('.');

                return ((idx >= 0) ? taskName.substring(idx + 1) : taskName).toLowerCase().contains(s);
            }

            return taskName.toLowerCase().contains(s);
        }

        /**
         * Filter events containing visor in it's name.
         *
         * @param e Event
         * @param taskName Task name to filter of events.
         * @return {@code true} if not contains {@code visor} in task name.
         */
        private boolean filterByTaskName(Event e, String taskName) {
            String compareTaskName = taskName.toLowerCase();

            if (e.getClass().equals(TaskEvent.class)) {
                TaskEvent te = (TaskEvent)e;

                return containsInTaskName(te.taskName(), te.taskClassName(), compareTaskName);
            }

            if (e.getClass().equals(JobEvent.class)) {
                JobEvent je = (JobEvent)e;

                return containsInTaskName(je.taskName(), je.taskName(), compareTaskName);
            }

            if (e.getClass().equals(DeploymentEvent.class)) {
                DeploymentEvent de = (DeploymentEvent)e;

                return de.alias().toLowerCase().contains(compareTaskName);
            }

            return true;
        }

        /**
         * Filter events containing visor in it's name.
         *
         * @param e Event
         * @return {@code true} if not contains {@code visor} in task name.
         */
        private boolean filterByTaskSessionId(Event e, IgniteUuid taskSesId) {
            if (e.getClass().equals(TaskEvent.class)) {
                TaskEvent te = (TaskEvent)e;

                return te.taskSessionId().equals(taskSesId);
            }

            if (e.getClass().equals(JobEvent.class)) {
                JobEvent je = (JobEvent)e;

                return je.taskSessionId().equals(taskSesId);
            }

            return true;
        }

        /**
         * @return Events mapper.
         */
        protected VisorEventMapper eventMapper() {
            return EVT_MAPPER;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends VisorGridEvent> run(final VisorNodeEventsCollectorTaskArg arg) {
            final long startEvtTime = arg.getTimeArgument() == null ? 0L : System.currentTimeMillis() - arg.getTimeArgument();

            final ConcurrentMap<String, Long> nl = ignite.cluster().nodeLocalMap();

            final Long startEvtOrder = arg.getKeyOrder() != null && nl.containsKey(arg.getKeyOrder()) ?
                nl.get(arg.getKeyOrder()) : -1L;

            Collection<Event> evts = ignite.events().localQuery(new IgnitePredicate<Event>() {
                /** */
                private static final long serialVersionUID = 0L;

                @Override public boolean apply(Event evt) {
                    return evt.localOrder() > startEvtOrder &&
                        (arg.getTypeArgument() == null || F.contains(arg.getTypeArgument(), evt.type())) &&
                        (evt.timestamp() >= startEvtTime) &&
                        (arg.getTaskName() == null || filterByTaskName(evt, arg.getTaskName())) &&
                        (arg.getTaskSessionId() == null || filterByTaskSessionId(evt, arg.getTaskSessionId()));
                }
            });

            Collection<VisorGridEvent> res = new ArrayList<>(evts.size());

            Long maxOrder = startEvtOrder;

            IgniteClosure<Event, VisorGridEvent> mapper = eventMapper();

            for (Event e : evts) {
                maxOrder = Math.max(maxOrder, e.localOrder());

                VisorGridEvent visorEvt = mapper.apply(e);

                if (visorEvt != null)
                    res.add(visorEvt);
                else
                    res.add(new VisorGridEvent(
                        e.type(), e.id(), e.name(), e.node().id(), e.timestamp(), e.message(), e.shortDisplay()
                    ));
            }

            // Update latest order in node local, if not empty.
            if (arg.getKeyOrder() != null && !res.isEmpty())
                nl.put(arg.getKeyOrder(), maxOrder);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodeEventsCollectorJob.class, this);
        }
    }
}
