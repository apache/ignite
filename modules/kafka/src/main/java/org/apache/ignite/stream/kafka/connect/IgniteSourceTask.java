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

package org.apache.ignite.stream.kafka.connect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to consume remote cluster cache events from the grid and inject them into Kafka.
 * <p>
 * Note that a task will create a bounded queue in the grid for more reliable data transfer.
 * Queue size can be changed by {@link IgniteSourceConstants#INTL_BUF_SIZE}.
 */
public class IgniteSourceTask extends SourceTask {
    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSourceTask.class);

    /** Tasks static monitor. */
    private static final Object lock = new Object();

    /** Event buffer size. */
    private static int evtBufSize = 100000;

    /** Event buffer. */
    private static BlockingQueue<CacheEvent> evtBuf = new LinkedBlockingQueue<>(evtBufSize);

    /** Max number of events taken from the buffer at once. */
    private static int evtBatchSize = 100;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Ignite grid configuration file. */
    private static String igniteCfgFile;

    /** Cache name. */
    private static String cacheName;

    /** Remote Listener id. */
    private static UUID rmtLsnrId;

    /** Local listener. */
    private static TaskLocalListener locLsnr = new TaskLocalListener();

    /** User-defined filter. */
    private static IgnitePredicate<CacheEvent> filter;

    /** Topic. */
    private static String topics[];

    /** Offset. */
    private static final Map<String, Long> offset = Collections.singletonMap("offset", 0L);

    /** Partition. */
    private static final Map<String, String> srcPartition = Collections.singletonMap("cache", null);

    /** {@inheritDoc} */
    @Override public String version() {
        return new IgniteSinkConnector().version();
    }

    /**
     * Filtering is done remotely. Local listener buffers data for injection into Kafka.
     *
     * @param props Task properties.
     */
    @Override public void start(Map<String, String> props) {
        synchronized (lock) {
            // Each task has the same parameters -- avoid setting more than once.
            // Nothing to do if the task has been already started.
            if (!stopped)
                return;

            cacheName = props.get(IgniteSourceConstants.CACHE_NAME);
            igniteCfgFile = props.get(IgniteSourceConstants.CACHE_CFG_PATH);
            topics = props.get(IgniteSourceConstants.TOPIC_NAMES).split("\\s*,\\s*");

            if (props.containsKey(IgniteSourceConstants.INTL_BUF_SIZE))
                evtBufSize = Integer.parseInt(props.get(IgniteSourceConstants.INTL_BUF_SIZE));

            if (props.containsKey(IgniteSourceConstants.INTL_BATCH_SIZE))
                evtBatchSize = Integer.parseInt(props.get(IgniteSourceConstants.INTL_BATCH_SIZE));

            if (props.containsKey(IgniteSourceConstants.CACHE_FILTER_CLASS)) {
                String filterCls = props.get(IgniteSourceConstants.CACHE_FILTER_CLASS);
                if (filterCls != null && !filterCls.isEmpty()) {
                    try {
                        Class<? extends IgnitePredicate<CacheEvent>> clazz =
                            (Class<? extends IgnitePredicate<CacheEvent>>)Class.forName(filterCls);

                        filter = clazz.newInstance();
                    }
                    catch (Exception e) {
                        log.error("Failed to instantiate the provided filter! " +
                            "User-enabled filtering is ignored!", e);
                    }
                }
            }

            TaskRemoteFilter rmtLsnr = new TaskRemoteFilter(cacheName);

            try {
                int[] evts = cacheEvents(props.get(IgniteSourceConstants.CACHE_EVENTS));

                rmtLsnrId = IgniteGrid.getIgnite().events(IgniteGrid.getIgnite().cluster().forCacheNodes(cacheName))
                    .remoteListen(locLsnr, rmtLsnr, evts);
            }
            catch (Exception e) {
                log.error("Failed to register event listener!", e);

                throw new ConnectException(e);
            }
            finally {
                stopped = false;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<>(evtBatchSize);
        ArrayList<CacheEvent> evts = new ArrayList<>(evtBatchSize);

        if (stopped)
            return records;

        try {
            if (evtBuf.drainTo(evts, evtBatchSize) > 0) {
                for (CacheEvent evt : evts) {
                    // schema and keys are ignored.
                    for (String topic : topics)
                        records.add(new SourceRecord(srcPartition, offset, topic, null, evt));
                }

                return records;
            }
        }
        catch (IgniteException e) {
            log.error("Error when polling event queue!", e);
        }

        // for shutdown.
        return null;
    }

    /**
     * Converts comma-delimited cache events strings to Ignite internal representation.
     *
     * @param evtPropsStr Comma-delimited cache event names.
     * @return Ignite internal representation of cache events to be registered with the remote listener.
     * @throws Exception If error.
     */
    private int[] cacheEvents(String evtPropsStr) throws Exception {
        String[] evtStr = evtPropsStr.split("\\s*,\\s*");

        if (evtStr.length == 0)
            return EventType.EVTS_CACHE;

        int[] evts = new int[evtStr.length];

        try {
            for (int i = 0; i < evtStr.length; i++)
                evts[i] = CacheEvt.valueOf(evtStr[i].toUpperCase()).getId();
        }
        catch (Exception e) {
            log.error("Failed to recognize the provided cache event!", e);

            throw new Exception(e);
        }
        return evts;
    }

    /**
     * Stops the grid client.
     */
    @Override public synchronized void stop() {
        if (stopped)
            return;

        stopped = true;

        stopRemoteListen();

        IgniteGrid.getIgnite().close();
    }

    /**
     * Stops the remote listener.
     */
    protected void stopRemoteListen() {
        if (rmtLsnrId != null)
            IgniteGrid.getIgnite().events(IgniteGrid.getIgnite().cluster().forCacheNodes(cacheName))
                .stopRemoteListen(rmtLsnrId);

        rmtLsnrId = null;
    }

    /**
     * Used by unit test to avoid restart node and valid state of the <code>stopped</code> flag.
     *
     * @param stopped Stopped flag.
     */
    protected static void setStopped(boolean stopped) {
        IgniteSourceTask.stopped = stopped;
    }

    /**
     * Local listener buffering cache events to be further sent to Kafka.
     */
    private static class TaskLocalListener implements IgniteBiPredicate<UUID, CacheEvent> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID id, CacheEvent evt) {
            try {
                if (!evtBuf.offer(evt, 10, TimeUnit.MILLISECONDS))
                    log.error("Failed to buffer event {}", evt.name());
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            return true;
        }
    }

    /**
     * Remote filter.
     */
    private static class TaskRemoteFilter implements IgnitePredicate<CacheEvent> {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** Cache name. */
        private final String cacheName;

        /**
         * @param cacheName Cache name.
         */
        TaskRemoteFilter(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(CacheEvent evt) {
            Affinity<Object> affinity = ignite.affinity(cacheName);

            if (affinity.isPrimary(ignite.cluster().localNode(), evt.key())) {
                // Process this event. Ignored on backups.
                if (filter != null && filter.apply(evt))
                    return false;

                return true;
            }

            return false;
        }
    }

    /**
     * Grid instance initialized on demand.
     */
    private static class IgniteGrid {
        /** Constructor. */
        private IgniteGrid() {
            // No-op.
        }

        /** Instance holder. */
        private static class Holder {
            /** */
            private static final Ignite IGNITE = Ignition.start(igniteCfgFile);
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        private static Ignite getIgnite() {
            return Holder.IGNITE;
        }
    }

    /** Cache events available for listening. */
    private enum CacheEvt {
        /** */
        CREATED(EventType.EVT_CACHE_ENTRY_CREATED),
        /** */
        DESTROYED(EventType.EVT_CACHE_ENTRY_DESTROYED),
        /** */
        PUT(EventType.EVT_CACHE_OBJECT_PUT),
        /** */
        READ(EventType.EVT_CACHE_OBJECT_READ),
        /** */
        REMOVED(EventType.EVT_CACHE_OBJECT_REMOVED),
        /** */
        LOCKED(EventType.EVT_CACHE_OBJECT_LOCKED),
        /** */
        UNLOCKED(EventType.EVT_CACHE_OBJECT_UNLOCKED),
        /** */
        EXPIRED(EventType.EVT_CACHE_OBJECT_EXPIRED);

        /** Internal Ignite event id. */
        private final int id;

        /**
         * Constructor.
         *
         * @param id Internal Ignite event id.
         */
        CacheEvt(int id) {
            this.id = id;
        }

        /**
         * Gets Ignite event id.
         *
         * @return Ignite event id.
         */
        int getId() {
            return id;
        }
    }
}
