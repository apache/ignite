/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.event.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Task that runs on specified node and returns events data.
 */
@GridInternal
public class VisorEventsCollectTask extends VisorMultiNodeTask<VisorEventsCollectTask.VisorEventsCollectArgs,
    Iterable<? extends VisorGridEvent>, Collection<? extends VisorGridEvent>> {
    /** {@inheritDoc} */
    @Override protected VisorEventsCollectJob job(VisorEventsCollectArgs arg) {
        return new VisorEventsCollectJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Iterable<? extends VisorGridEvent> reduce(
        List<GridComputeJobResult> results) throws GridException {

        Collection<VisorGridEvent> allEvents = new ArrayList<>();

        for (GridComputeJobResult r : results) {
            if (r.getException() == null)
                allEvents.addAll((Collection<VisorGridEvent>) r.getData());
        }

        return allEvents.isEmpty() ? null : allEvents;
    }

    /**
     * Argument for task returns events data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorEventsCollectArgs implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Arguments for type filter. */
        private final int[] typeArg;

        /** Arguments for time filter. */
        private final Long timeArg;

        /** Task or job events with task name contains. */
        private final String taskName;

        /** Task or job events with session. */
        private final GridUuid taskSessionId;

        /**
         * @param typeArg Arguments for type filter.
         * @param timeArg Arguments for time filter.
         * @param taskName Arguments for task name filter.
         * @param taskSessionId Arguments for task session filter.
         */
        public VisorEventsCollectArgs(@Nullable int[] typeArg, @Nullable Long timeArg, @Nullable String taskName,
            @Nullable GridUuid taskSessionId) {
            this.typeArg = typeArg;
            this.timeArg = timeArg;
            this.taskName = taskName;
            this.taskSessionId = taskSessionId;
        }

        /**
         * @param typeArg Arguments for type filter.
         * @param timeArg Arguments for time filter.
         */
        public static VisorEventsCollectArgs createEventsArg(@Nullable int[] typeArg, @Nullable Long timeArg) {
            return new VisorEventsCollectArgs(typeArg, timeArg, "visor", null);
        }

        /**
         * @param timeArg Arguments for time filter.
         * @param taskName Arguments for task name filter.
         * @param taskSessionId Arguments for task session filter.
         */
        public static VisorEventsCollectArgs createTasksArg(@Nullable Long timeArg, @Nullable String taskName,
            @Nullable GridUuid taskSessionId) {
            return new VisorEventsCollectArgs(
                VisorTaskUtils.concat(GridEventType.EVTS_JOB_EXECUTION, GridEventType.EVTS_TASK_EXECUTION),
                timeArg, taskName, taskSessionId);
        }

        /**
         * @return Arguments for type filter.
         */
        public int[] typeArgument() {
            return typeArg;
        }

        /**
         * @return Arguments for time filter.
         */
        public Long timeArgument() {
            return timeArg;
        }

        /**
         * @return Task or job events with task name contains.
         */
        public String taskName() {
            return taskName;
        }

        /**
         * @return Task or job events with session.
         */
        public GridUuid taskSessionId() {
            return taskSessionId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorEventsCollectArgs.class, this);
        }
    }

    /**
     * Job for task returns events data.
     */
    private static class VisorEventsCollectJob extends VisorJob<VisorEventsCollectArgs,
        Collection<? extends VisorGridEvent>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        private VisorEventsCollectJob(VisorEventsCollectArgs arg) {
            super(arg);
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
         * @return {@code true} if not contains {@code visor} in task name.
         */
        private boolean filterByTaskName(GridEvent e, String taskName) {
            if (e.getClass().equals(GridTaskEvent.class)) {
                GridTaskEvent te = (GridTaskEvent)e;

                return containsInTaskName(te.taskName(), te.taskClassName(), taskName);
            }

            if (e.getClass().equals(GridJobEvent.class)) {
                GridJobEvent je = (GridJobEvent)e;

                return containsInTaskName(je.taskName(), je.taskName(), taskName);
            }

            if (e.getClass().equals(GridDeploymentEvent.class)) {
                GridDeploymentEvent de = (GridDeploymentEvent)e;

                return de.alias().toLowerCase().contains(taskName);
            }

            return true;
        }

        /**
         * Filter events containing visor in it's name.
         *
         * @param e Event
         * @return {@code true} if not contains {@code visor} in task name.
         */
        private boolean filterByTaskSessionId(GridEvent e, GridUuid taskSessionId) {
            if (e.getClass().equals(GridTaskEvent.class)) {
                GridTaskEvent te = (GridTaskEvent)e;

                return te.taskSessionId().equals(taskSessionId);
            }

            if (e.getClass().equals(GridJobEvent.class)) {
                GridJobEvent je = (GridJobEvent)e;

                return je.taskSessionId().equals(taskSessionId);
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends VisorGridEvent> run(final VisorEventsCollectArgs arg)
            throws GridException {
            final long startEvtTime = arg.timeArgument() == null ? 0L : System.currentTimeMillis() - arg.timeArgument();

            Collection<GridEvent> evts = g.events().localQuery(new GridPredicate<GridEvent>() {
                private Map<String, Boolean> internalTasks = new HashMap<>();

                /**
                 * TODO-8003 Remove after fix.
                 * Detects internal task or job.
                 *
                 * @param e Event
                 * @return {@code true} if internal.
                 */
                private boolean internal(GridEvent e) {
                    if (e.getClass().equals(GridTaskEvent.class)) {
                        GridTaskEvent te = (GridTaskEvent)e;

                        internalTasks.put(te.taskClassName(), te.internal());

                        return te.internal();
                    }

                    if (e.getClass().equals(GridJobEvent.class)) {
                        GridJobEvent je = (GridJobEvent)e;

                        Boolean internal = internalTasks.get(je.taskClassName());

                        if (internal != null)
                            return internal;

                        try {
                            internal = U.hasAnnotation(Class.forName(je.taskClassName()), GridInternal.class);
                        }
                        catch (Exception ignored) {
                            internal = true;
                        }

                        internalTasks.put(je.taskClassName(), internal);

                        return internal;
                    }

                    return true;
                }

                @Override public boolean apply(GridEvent event) {
                    return (arg.typeArgument() == null || F.contains(arg.typeArgument(), event.type())) &&
                        event.timestamp() >= startEvtTime &&
                        !internal(event) &&
                        (arg.taskName() == null || filterByTaskName(event, arg.taskName())) &&
                        (arg.taskSessionId() == null || filterByTaskSessionId(event, arg.taskSessionId()));
                }
            });

            Collection<VisorGridEvent> res = new ArrayList<>(evts.size());

            for (GridEvent e : evts) {
                int tid = e.type();
                GridUuid id = e.id();
                String name = e.name();
                UUID nid = e.node().id();
                long t = e.timestamp();
                String msg = e.message();
                String shortDisplay = e.shortDisplay();

                if (e instanceof GridTaskEvent) {
                    GridTaskEvent te = (GridTaskEvent)e;

                    res.add(new VisorGridTaskEvent(tid, id, name, nid, t, msg, shortDisplay,
                        te.taskName(), te.taskClassName(), te.taskSessionId(), te.internal()));
                }
                else if (e instanceof GridJobEvent) {
                    GridJobEvent je = (GridJobEvent)e;

                    res.add(new VisorGridJobEvent(tid, id, name, nid, t, msg, shortDisplay,
                        je.taskName(), je.taskClassName(), je.taskSessionId(), je.jobId()));
                }
                else if (e instanceof GridDeploymentEvent) {
                    GridDeploymentEvent de = (GridDeploymentEvent)e;

                    res.add(new VisorGridDeploymentEvent(tid, id, name, nid, t, msg, shortDisplay, de.alias()));
                }
                else if (e instanceof GridLicenseEvent) {
                    GridLicenseEvent le = (GridLicenseEvent)e;

                    res.add(new VisorGridLicenseEvent(tid, id, name, nid, t, msg, shortDisplay, le.licenseId()));
                } else
                    res.add(new VisorGridEvent(tid, id, name, nid, t, msg, shortDisplay));
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorEventsCollectJob.class, this);
        }
    }
}
