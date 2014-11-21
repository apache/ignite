/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.dto.*;
import org.gridgain.grid.kernal.visor.dto.event.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;
import static org.gridgain.grid.kernal.visor.dto.VisorComputeMonitoringHolder.*;
import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Collects current Grid state mostly topology and metrics.
 */
@GridInternal
public class VisorDataCollectorTask extends VisorMultiNodeTask<VisorDataCollectorTask.VisorDataCollectorTaskArg,
        VisorDataCollectorTask.VisorDataCollectorTaskResult, VisorDataCollectorTask.VisorDataCollectorJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid,
        @Nullable GridBiTuple<Set<UUID>, VisorDataCollectorTaskArg> arg) throws GridException {
        assert arg != null;
        assert arg.get1() != null;

        taskArg = arg.get2();

        Map<GridComputeJob, GridNode> map = new HashMap<>();

        // Collect data from ALL nodes.
        for (GridNode node : g.nodes())
            map.put(job(taskArg), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override protected VisorDataCollectorJob job(VisorDataCollectorTaskArg arg) {
        return new VisorDataCollectorJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public VisorDataCollectorTaskResult reduce(List<GridComputeJobResult> results) throws GridException {
        VisorDataCollectorTaskResult data = new VisorDataCollectorTaskResult();

        for (GridComputeJobResult res : results) {
            VisorDataCollectorJobResult jobData = res.getData();

            if (jobData != null) {
                UUID nid = res.getNode().id();

                GridException unhandledEx = res.getException();

                if (unhandledEx == null) {
                    data.gridNames.put(nid, jobData.gridName);

                    data.topologyVersions.put(nid, jobData.topologyVersion);

                    data.taskMonitoringEnabled.put(nid, jobData.taskMonitoringEnabled);

                    if (!jobData.events.isEmpty())
                        data.events.addAll(jobData.events);

                    if (jobData.eventsEx != null)
                        data.eventsEx.put(nid, jobData.eventsEx);

                    if (jobData.license != null)
                        data.licenses.put(nid, jobData.license);

                    if (jobData.licenseEx != null)
                        data.licensesEx.put(nid, jobData.licenseEx);

                    if (!jobData.caches.isEmpty())
                        data.caches.put(nid, jobData.caches);

                    if (jobData.cachesEx != null)
                        data.cachesEx.put(nid, jobData.cachesEx);

                    if (!jobData.streamers.isEmpty())
                        data.streamers.put(nid, jobData.streamers);

                    if (jobData.streamersEx != null)
                        data.streamersEx.put(nid, jobData.streamersEx);

                    if (!jobData.ggfss.isEmpty())
                        data.ggfss.put(nid, jobData.ggfss);

                    if (!jobData.ggfsEndpoints.isEmpty())
                        data.ggfsEndpoints.put(nid, jobData.ggfsEndpoints);

                    if (jobData.ggfssEx != null)
                        data.ggfssEx.put(nid, jobData.ggfssEx);

                    if (jobData.dr != null)
                        data.drs.put(nid, jobData.dr);

                    if (jobData.drEx != null)
                        data.drsEx.put(nid, jobData.drEx);

                    // TODO: gg-mongo if (jobData.mongo != null)
                    //      data.mongos.put(nid, jobData.mongo);

                    //   if (jobData.mongoEx != null)
                    // TODO: gg-mongo data.mongosEx.put(nid, jobData.mongoEx)
                }
                else {
                    // Ignore nodes that left topology.
                    if (!(unhandledEx instanceof GridEmptyProjectionException))
                        data.unhandledEx.put(nid, unhandledEx);
                }
            }
        }

        return data;
    }

    /**
     * Arguments for {@link VisorDataCollectorTask}
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorDataCollectorTaskArg implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Whether task monitoring should be enabled. */
        private final boolean taskMonitoringEnabled;

        /** Visor unique key to get last event order from node local storage. */
        private final String evtOrderKey;

        /** Visor unique key to get lost events throttle counter from node local storage. */
        private final String evtThrottleCntrKey;

        /**
         * Whether cache sampling enabled in Visor preferences.
         * This parameter was not supported anymore and will be ignored.
         * Should be removed on next-breaking-compatibility release.
         */
        @Deprecated
        private final boolean samplingEnabled;

        /** cache sample size. */
        private final int sample;

        /**
         * Create task arguments with given parameters.
         *
         * @param taskMonitoringEnabled Required task monitoring state.
         * @param evtOrderKey Event order key, unique for Visor instance.
         * @param evtThrottleCntrKey Event throttle counter key, unique for Visor instance.
         * @param samplingEnabled Whether to perform cache sampling.
         * @param sample How many entries use in sampling.
         */
        public VisorDataCollectorTaskArg(boolean taskMonitoringEnabled, String evtOrderKey,
            String evtThrottleCntrKey, boolean samplingEnabled, int sample) {
            this.taskMonitoringEnabled = taskMonitoringEnabled;
            this.evtOrderKey = evtOrderKey;
            this.evtThrottleCntrKey = evtThrottleCntrKey;
            this.samplingEnabled = samplingEnabled;
            this.sample = sample;
        }
    }

    /**
     * Data collector task result.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorDataCollectorTaskResult implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        public static final VisorDataCollectorTaskResult EMPTY = new VisorDataCollectorTaskResult();

        /** Unhandled exceptions from nodes. */
        private final Map<UUID, Throwable> unhandledEx = new HashMap<>();

        /** Nodes grid names. */
        private final Map<UUID, String> gridNames = new HashMap<>();

        /** Nodes topology versions. */
        private final Map<UUID, Long> topologyVersions = new HashMap<>();

        /** All task monitoring state collected from nodes. */
        private final Map<UUID, Boolean> taskMonitoringEnabled = new HashMap<>();

        /** All events collected from nodes. */
        private final List<VisorGridEvent> events = new ArrayList<>();

        /** Exceptions caught during collecting events from nodes. */
        private final Map<UUID, Throwable> eventsEx = new HashMap<>();

        /** All licenses collected from nodes. */
        private final Map<UUID, VisorLicense> licenses = new HashMap<>();

        /** Exceptions caught during collecting licenses from nodes. */
        private final Map<UUID, Throwable> licensesEx = new HashMap<>();

        /** All caches collected from nodes. */
        private final Map<UUID, Collection<VisorCache>> caches = new HashMap<>();

        /** Exceptions caught during collecting caches from nodes. */
        private final Map<UUID, Throwable> cachesEx = new HashMap<>();

        /** All GGFS collected from nodes. */
        private final Map<UUID, Collection<VisorGgfs>> ggfss = new HashMap<>();

        /** All GGFS endpoints collected from nodes. */
        private final Map<UUID, Collection<VisorGgfsEndpoint>> ggfsEndpoints = new HashMap<>();

        /** Exceptions caught during collecting GGFS from nodes. */
        private final Map<UUID, Throwable> ggfssEx = new HashMap<>();

        /** All streamers collected from nodes. */
        private final Map<UUID, Collection<VisorStreamer>> streamers = new HashMap<>();

        /** Exceptions caught during collecting streamers from nodes. */
        private final Map<UUID, Throwable> streamersEx = new HashMap<>();

        /** All DR collected from nodes. */
        private final Map<UUID, VisorDr> drs = new HashMap<>();

        /** Exceptions caught during collecting DRs from nodes. */
        private final Map<UUID, Throwable> drsEx = new HashMap<>();

        /** All mongos collected from nodes. */
        // TODO: gg-mongo private final Map<UUID, VisorMongo> mongos= new HashMap<>();

        /** Exceptions caught during collecting mongos from nodes. */
        // TODO: gg-mongo private final Map<UUID, Throwable> mongosEx = new HashMap<>();

        /**
         * @return {@code true} If no data was collected.
         */
        public boolean isEmpty() {
            return
                gridNames.isEmpty() &&
                topologyVersions.isEmpty() &&
                unhandledEx.isEmpty() &&
                taskMonitoringEnabled.isEmpty() &&
                events.isEmpty() &&
                eventsEx.isEmpty() &&
                licenses.isEmpty() &&
                licensesEx.isEmpty() &&
                caches.isEmpty() &&
                cachesEx.isEmpty() &&
                ggfss.isEmpty() &&
                ggfsEndpoints.isEmpty() &&
                ggfssEx.isEmpty() &&
                streamers.isEmpty() &&
                streamersEx.isEmpty() &&
                drs.isEmpty() &&
                drsEx.isEmpty();
            // TODO: gg-mongo mongos.isEmpty() &&
            // TODO: gg-mongo mongosEx.isEmpty() &&
        }

        /**
         * @return Unhandled exceptions from nodes.
         */
        public Map<UUID, Throwable> unhandledEx() {
            return unhandledEx;
        }

        /**
         * @return Nodes grid names.
         */
        public Map<UUID, String> gridNames() {
            return gridNames;
        }

        /**
         * @return Nodes topology versions.
         */
        public Map<UUID, Long> topologyVersions() {
            return topologyVersions;
        }

        /**
         * @return All task monitoring state collected from nodes.
         */
        public Map<UUID, Boolean> taskMonitoringEnabled() {
            return taskMonitoringEnabled;
        }

        /**
         * @return All events collected from nodes.
         */
        public List<VisorGridEvent> events() {
            return events;
        }

        /**
         * @return Exceptions caught during collecting events from nodes.
         */
        public Map<UUID, Throwable> eventsEx() {
            return eventsEx;
        }

        /**
         * @return All licenses collected from nodes.
         */
        public Map<UUID, VisorLicense> licenses() {
            return licenses;
        }

        /**
         * @return Exceptions caught during collecting licenses from nodes.
         */
        public Map<UUID, Throwable> licensesEx() {
            return licensesEx;
        }

        /**
         * @return All caches collected from nodes.
         */
        public Map<UUID, Collection<VisorCache>> caches() {
            return caches;
        }

        /**
         * @return Exceptions caught during collecting caches from nodes.
         */
        public Map<UUID, Throwable> cachesEx() {
            return cachesEx;
        }

        /**
         * @return All GGFS collected from nodes.
         */
        public Map<UUID, Collection<VisorGgfs>> ggfss() {
            return ggfss;
        }

        /**
         * @return All GGFS endpoints collected from nodes.
         */
        public Map<UUID, Collection<VisorGgfsEndpoint>> ggfsEndpoints() {
            return ggfsEndpoints;
        }

        /**
         * @return Exceptions caught during collecting GGFS from nodes.
         */
        public Map<UUID, Throwable> ggfssEx() {
            return ggfssEx;
        }

        /**
         * @return All streamers collected from nodes.
         */
        public Map<UUID, Collection<VisorStreamer>> streamers() {
            return streamers;
        }

        /**
         * @return Exceptions caught during collecting streamers from nodes.
         */
        public Map<UUID, Throwable> streamersEx() {
            return streamersEx;
        }

        /**
         * @return All DR collected from nodes.
         */
        public Map<UUID, VisorDr> drs() {
            return drs;
        }

        /**
         * @return Exceptions caught during collecting DRs from nodes.
         */
        public Map<UUID, Throwable> drsEx() {
            return drsEx;
        }
    }

    /**
     * Data collector job result.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorDataCollectorJobResult implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Grid name. */
        private String gridName;

        /** Node topology version. */
        private long topologyVersion;

        /** Task monitoring state collected from node. */
        private boolean taskMonitoringEnabled;

        /** Node events.*/
        private final Collection<VisorGridEvent> events = new ArrayList<>();
        /** Exception while collecting node events.*/
        private Throwable eventsEx;

        /** Node license.*/
        private VisorLicense license;
        /** Exception while collecting node license.*/
        private Throwable licenseEx;

        /** Node caches. */
        private final Collection<VisorCache> caches = new ArrayList<>();
        /** Exception while collecting node caches.*/
        private Throwable cachesEx;

        /** Node GGFSs. */
        private final Collection<VisorGgfs> ggfss = new ArrayList<>();
        /** All GGFS endpoints collected from nodes. */
        private final Collection<VisorGgfsEndpoint> ggfsEndpoints = new ArrayList<>();
        /** Exception while collecting node GGFSs.*/
        private Throwable ggfssEx;

        /** Node streamers. */
        private final Collection<VisorStreamer> streamers = new ArrayList<>();
        /** Exception while collecting node streamers.*/
        private Throwable streamersEx;

        /** Node DR. */
        private VisorDr dr;
        /** Exception while collecting node DR.*/
        private Throwable drEx;

        /** Node Mongo. */
        // TODO: gg-mongo private VisorMongo mongo;
        /** Exception while collecting node mongo.*/
        // TODO: gg-mongo private Throwable mongoEx;
    }

    /**
     * Job that collects data from node.
     */
    private static class VisorDataCollectorJob extends VisorJob<VisorDataCollectorTaskArg, VisorDataCollectorJobResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         */
        private VisorDataCollectorJob(VisorDataCollectorTaskArg arg) {
            super(arg);
        }

        /** Collect events. */
        private void events(VisorDataCollectorJobResult res, VisorDataCollectorTaskArg arg) {
            try {
                // Visor events explicitly enabled in configuration.
                if (checkExplicitTaskMonitoring(g))
                    res.taskMonitoringEnabled = true;
                else {
                    // Get current task monitoring state.
                    res.taskMonitoringEnabled = arg.taskMonitoringEnabled;

                    if (arg.taskMonitoringEnabled) {
                        GridNodeLocalMap<String, VisorComputeMonitoringHolder> storage = g.nodeLocalMap();

                        VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

                        if (holder == null) {
                            VisorComputeMonitoringHolder holderNew = new VisorComputeMonitoringHolder();

                            VisorComputeMonitoringHolder holderOld = storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holderNew);

                            holder = holderOld == null ? holderNew : holderOld;
                        }

                        // Enable task monitoring for new node in grid.
                        holder.startCollect(g, arg.evtOrderKey);

                        // Update current state after change (it may not changed in some cases).
                        res.taskMonitoringEnabled = g.allEventsUserRecordable(VISOR_TASK_EVTS);
                    }
                }

                res.events.addAll(collectEvents(g, arg.evtOrderKey, arg.evtThrottleCntrKey, arg.taskMonitoringEnabled));
            }
            catch(Throwable eventsEx) {
                res.eventsEx = eventsEx;
            }
        }

        /** Collect license. */
        private void license(VisorDataCollectorJobResult res) {
            if (ENT)
                try {
                    // If license could not be retrieved, try to let it load for 5 time.
                    for (int i = 0; i < 5; i++) {
                        res.license = VisorLicense.from(g);

                        if (res.license != null)
                            break;

                        U.sleep(1000);
                    }
                }
                catch(Throwable licenseEx) {
                    res.licenseEx = licenseEx;
                }
        }

        /** Collect caches. */
        private void caches(VisorDataCollectorJobResult res, VisorDataCollectorTaskArg arg) {
            try {
                for (GridCache cache : g.cachesx()) {
                    res.caches.add(VisorCache.from(g, cache, arg.sample));
                }
            }
            catch(Throwable cachesEx) {
                res.cachesEx = cachesEx;
            }
        }

        /** Collect GGFS. */
        private void ggfs(VisorDataCollectorJobResult res) {
            try {
                GridGgfsProcessorAdapter ggfsProc = ((GridKernal)g).context().ggfs();

                for (GridGgfs ggfs : ggfsProc.ggfss()) {
                    Collection<GridIpcServerEndpoint> endPoints = ggfsProc.endpoints(ggfs.name());

                    if (endPoints != null) {
                        for (GridIpcServerEndpoint ep : endPoints)
                            if (ep.isManagement())
                                res.ggfsEndpoints.add(new VisorGgfsEndpoint(ggfs.name(), g.name(),
                                    ep.getHost(), ep.getPort()));
                    }

                    res.ggfss.add(VisorGgfs.from(ggfs));
                }
            }
            catch(Throwable ggfssEx) {
                res.ggfssEx = ggfssEx;
            }
        }

        /** Collect streamers. */
        private void streamers(VisorDataCollectorJobResult res) {
            try {
                GridStreamerConfiguration[] cfgs = g.configuration().getStreamerConfiguration();

                if (cfgs != null) {
                    for (GridStreamerConfiguration cfg : cfgs) {
                        res.streamers.add(VisorStreamer.from(g.streamer(cfg.getName())));
                    }
                }
            }
            catch(Throwable streamersEx) {
                res.streamersEx = streamersEx;
            }
        }

        /** Collect DR. */
        private void dr(VisorDataCollectorJobResult res) {
            if (ENT) // Collect DR only for Enterprise edition.
                try {
                    if (g.dr() != null)
                        res.dr = VisorDr.from(g);
                }
                catch(Throwable drEx) {
                    res.drEx = drEx;
                }
        }

        // TODO: gg-mongo private void mongo(VisorDataCollectorJobResult res) {
        //            try {
        //                GridMongo mongo = g.mongo();
        //
        //                if (mongo != null)
        //                    res.mongo = VisorMongo.create(mongo);
        //            } catch (Throwable mongoEx) {
        //                res.mongoEx = mongoEx;
        //            }
        // TODO: gg-mongo }

        /** {@inheritDoc} */
        @Override protected VisorDataCollectorJobResult run(VisorDataCollectorTaskArg arg) throws GridException {
            VisorDataCollectorJobResult res = new VisorDataCollectorJobResult();

            res.gridName = g.name();

            res.topologyVersion = g.topologyVersion();

            events(res, arg);

            license(res);

            caches(res, arg);

            ggfs(res);

            streamers(res);

            dr(res);

            // TODO: gg-mongo mongo(res);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorDataCollectorJob.class, this);
        }
    }
}
