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

package org.apache.ignite.source.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Apache Flink Ignite source implemented as a RichParallelSourceFunction.
 */
public class IgniteSource extends RichParallelSourceFunction<CacheEvent> {
    /** Serial version uid. */
    private static final long serialVersionUID = 1L;

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSource.class);

    /** Event buffer. */
    private static BlockingQueue<CacheEvent> evtBuf = new LinkedBlockingQueue<>();

    /** Default max number of events taken from the buffer at once. */
    private static final int DFLT_EVT_BATCH_SIZE = 100;

    /** Max number of events taken from the buffer at once. */
    private int evtBatchSize = DFLT_EVT_BATCH_SIZE;

    /**
     * Sets Event Batch Size.
     *
     * @param evtBatchSize Event Batch Size.
     */
    public void setEvtBatchSize(int evtBatchSize) {
        this.evtBatchSize = evtBatchSize;
    }

    /** Default number of milliseconds timeout for event buffer queue operation. */
    private static final int DFLT_EVT_BUFFER_TIMEOUT = 10;

    /** Number of milliseconds timeout for event buffer queue operation. */
    private int evtBufferTimeout = DFLT_EVT_BUFFER_TIMEOUT;

    /**
     * Sets Event Buffer timeout.
     *
     * @param evtBufferTimeout Event Buffer timeout.
     */
    public void setEvtBufferTimeout(int evtBufferTimeout) {
        this.evtBufferTimeout = evtBufferTimeout;
    }

    /** Remote Listener id. */
    private static UUID rmtLsnrId;

    /** Local listener. */
    private TaskLocalListener locLsnr = new TaskLocalListener();

    /** User-defined filter. */
   private static IgnitePredicate<CacheEvent> filter;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Ignite grid configuration file. */
    private String igniteCfgFile;

    /** Cache name. */
    private String cacheName;

    /** Ignite instance. **/
    private Ignite ignite;

    /**
     * Default IgniteSource constructor.
     *
     * @param cacheName Cache name.
     * @param igniteCfgFile Ignite configuration file.
     */
    public IgniteSource(String cacheName, String igniteCfgFile) {
        this.cacheName = cacheName;
        this.igniteCfgFile = igniteCfgFile;
    }

    /**
     * Starts Ignite source.
     * @param filterCls User defined filter class name.
     * @param cacheEvents Converts comma-delimited cache events strings to Ignite internal representation.
     *
     * @throws IgniteException If failed.
     */
    @SuppressWarnings("unchecked")
    public void start(String filterCls, String cacheEvents) throws Exception {
        A.notNull(igniteCfgFile, "Ignite config file");
        A.notNull(cacheName, "Cache name");

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

        ignite = new IgniteContext(igniteCfgFile).getIgnite();

        TaskRemoteFilter rmtLsnr = new TaskRemoteFilter(cacheName);

        try {
            int[] evts = cacheEvents(cacheEvents);

            rmtLsnrId = ignite.events(ignite.cluster().forCacheNodes(cacheName))
                .remoteListen(locLsnr, rmtLsnr, evts);
        }
        catch (Exception e) {
            log.error("Failed to register event listener!", e);

            throw e;
        }
        finally {
            stopped = false;
        }
        stopped = false;
    }

    /**
     * Stops Ignite source.
     *
     * @throws IgniteException If failed.
     */
    public void stop() throws IgniteException {
        if (stopped)
            return;

        stopped = true;

        if (rmtLsnrId != null)
            ignite.events(ignite.cluster().forCacheNodes(cacheName))
                 .stopRemoteListen(rmtLsnrId);

        rmtLsnrId = null;

        ignite.cache(cacheName).close();
    }

    /**
     * Transfers data from grid.
     *
     * @param ctx SourceContext.
     */
    @Override public void run(SourceContext<CacheEvent> ctx) {
        List<CacheEvent> evts = new ArrayList<>(evtBatchSize);

        if (stopped)
            return;

        try{
          while (!stopped) {

              if (evtBuf.drainTo(evts, evtBatchSize) > 0) {
                  synchronized (ctx.getCheckpointLock()) {

                      for (CacheEvent evt : evts) {
                          ctx.collect(evt);
                      }
                  }
              }
          }
        } catch (Exception e){
            log.error("Error while processing cache event of " + cacheName, e);
        }
        return;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        stopped = true;
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
     * Ignite context initializing grid on demand.
     */
    private class IgniteContext {
        /** Ignite configuration file. */
        private String igniteCfgFile;

        /** Constructor.
         *
         * @param igniteCfgFile Ignite configuration file.
         * */
        private IgniteContext(String igniteCfgFile) {
            this.igniteCfgFile = igniteCfgFile;
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        private Ignite getIgnite() {
            return Ignition.start(igniteCfgFile);
        }
    }

    /**
     * Remote filter.
     */
    private static class TaskRemoteFilter implements IgnitePredicate<CacheEvent> {
        /** Ignite Instance Resource */
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

            // Process this event. Ignored on backups.
            return affinity.isPrimary(ignite.cluster().localNode(), evt.key()) &&
                !(filter != null && filter.apply(evt));
        }
    }

    /**
     * Local listener buffering cache events to be further sent to Flink.
     */
    private class TaskLocalListener implements IgniteBiPredicate<UUID, CacheEvent> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID id, CacheEvent evt) {
            try {
                if (!evtBuf.offer(evt, evtBufferTimeout, TimeUnit.MILLISECONDS))
                    log.error("Failed to buffer event {}", evt.name());
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            return true;
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
        SWAPPED(EventType.EVT_CACHE_OBJECT_SWAPPED),
        /** */
        UNSWAPPED(EventType.EVT_CACHE_OBJECT_UNSWAPPED),
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
