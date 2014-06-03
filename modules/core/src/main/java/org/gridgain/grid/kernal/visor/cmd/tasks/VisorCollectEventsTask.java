/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Task that runs on specified node and returns events data.
 */
@GridInternal
public class VisorCollectEventsTask extends VisorOneNodeTask<VisorCollectEventsTask.VisorCollectEventsArgs,
    Iterable<VisorCollectEventsTask.VisorEventData>> {
    /**
     * Argument for task returns events data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCollectEventsArgs extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** Arguments for type filter. */
        @Nullable private final int[] typeArg;

        /** Arguments for time filter. */
        @Nullable private final Long timeArg;

        /**
         * Arguments for {@link VisorCollectEventsTask}.
         *
         * @param nodeId Node Id where events should be collected.
         * @param typeArg Arguments for type filter.
         * @param timeArg Arguments for time filter.
         */
        public VisorCollectEventsArgs(UUID nodeId, @Nullable int[] typeArg, @Nullable Long timeArg) {
            super(nodeId);

            this.typeArg = typeArg;
            this.timeArg = timeArg;
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
    }

    /** Descriptor for {@link GridEvent}. */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorEventData implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String name;

        /** */
        private final String shortDisplay;

        /** */
        private final String mnemonic;

        /** */
        private final int type;

        /** */
        private final long timestamp;

        public VisorEventData(String name, String shortDisplay, String mnemonic, Integer type, long timestamp) {
            this.type = type;
            this.timestamp = timestamp;
            this.name = name;
            this.shortDisplay = shortDisplay;
            this.mnemonic = mnemonic;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return Short display.
         */
        public String shortDisplay() {
            return shortDisplay;
        }

        /**
         * @return Mnemonic.
         */
        public String mnemonic() {
            return mnemonic;
        }

        /**
         * @return Type.
         */
        public int type() {
            return type;
        }

        /**
         * @return Timestamp.
         */
        public long timestamp() {
            return timestamp;
        }
    }

    /**
     * Job for task returns events data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCollectEventsJob extends VisorJob<VisorCollectEventsArgs, Iterable<VisorEventData>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorCollectEventsJob(VisorCollectEventsArgs arg) {
            super(arg);
        }

        private void addArray(Collection<Integer> acc, int[] values) {
            for(Integer value : values)
                acc.add(value);
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
        private boolean filterVisor(GridEvent e) {
            if (e instanceof GridTaskEvent) {
                GridTaskEvent te = (GridTaskEvent)e;

                return !containsInTaskName(te.taskName(), te.taskClassName(), "visor");
            }

            if (e instanceof GridJobEvent) {
                GridJobEvent je = (GridJobEvent)e;

                return !containsInTaskName(je.taskName(), je.taskName(), "visor");
            }

            if (e instanceof GridDeploymentEvent) {
                GridDeploymentEvent de = (GridDeploymentEvent)e;

                return !de.alias().toLowerCase().contains("visor");
            }

            return true;
        }

        /**
         * Gets command's mnemonic for given event.
         *
         * @param e Event to get mnemonic for.
         */
        private String mnemonic(GridEvent e) {
            assert e != null;

            if (e.getClass().equals(GridDiscoveryEvent.class))
                return "di";

            if (e.getClass().equals(GridCheckpointEvent.class))
                return "ch";

            if (e.getClass().equals(GridDeploymentEvent.class))
                return "de";

            if (e.getClass().equals(GridJobEvent.class))
                return "jo";

            if (e.getClass().equals(GridTaskEvent.class))
                return "ta";

            if (e.getClass().equals(GridCacheEvent.class))
                return "ca";

            if (e.getClass().equals(GridSwapSpaceEvent.class))
                return "sw";

            if (e.getClass().equals(GridCachePreloadingEvent.class))
                return "cp";

            if (e.getClass().equals(GridAuthenticationEvent.class))
                return "au";

            // Should never happen.
            return null;
        }

        @Override protected Iterable<VisorEventData> run(final VisorCollectEventsArgs arg) throws GridException {
            final long startEvtTime = arg.timeArgument() == null ? 0L : System.currentTimeMillis() - arg.timeArgument();

            Collection<GridEvent> evts = g.events().localQuery(new GridPredicate<GridEvent>() {
                  @Override public boolean apply(GridEvent event) {
                    return (arg.typeArgument() == null || F.contains(arg.typeArgument(), event.type())) &&
                        event.timestamp() >= startEvtTime && filterVisor(event);
                  }
              }
            );

            Collection<VisorEventData> res = new ArrayList<>(evts.size());

            for (GridEvent e : evts) {
                String m = mnemonic(e);

                if (m != null)
                    res.add(new VisorEventData(e.name(), e.shortDisplay(), m, e.type(), e.timestamp()));
            }

            return res;
        }
    }

    @Override protected VisorCollectEventsJob job(VisorCollectEventsArgs arg) {
        return new VisorCollectEventsJob(arg);
    }
}
