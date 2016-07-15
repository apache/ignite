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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Apache Flink Ignite source implemented as a RichSourceFunction.
 */
public class IgniteSource extends RichSourceFunction<CacheEvent> {

    private static final long serialVersionUID = 1L;

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSource.class);

    /** Event buffer size. */
    private static int evtBufSize = 100000;

    /** Event buffer. */
    private static BlockingQueue<CacheEvent> evtBuf = new LinkedBlockingQueue<>(evtBufSize);

    /** Max number of events taken from the buffer at once. */
    private static int evtBatchSize = 100;

    /** Remote Listener id. */
    private static UUID rmtLsnrId;

    /** Local listener. */
    private static TaskLocalListener locLsnr = new TaskLocalListener();

    /** User-defined filter. */
    private static IgnitePredicate<CacheEvent> filter;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Ignite grid configuration file. */
    private static String igniteCfgFile;

    /** Cache name. */
    private static String cacheName;

    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Gets Ignite configuration file.
     *
     * @return Configuration file.
     */
    public String getIgniteConfigFile() {
        return igniteCfgFile;
    }

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
     * @param evtBufSize Event buffer size for the Blocking Queue.
     * @param evtBatchSize Event batch size to publish events.
     * @param cacheEvents Converts comma-delimited cache events strings to Ignite internal representation.
     *
     * @throws IgniteException If failed.
     */
    @SuppressWarnings("unchecked")
    public void start(int evtBufSize, int evtBatchSize, String cacheEvents) throws Exception {
        A.notNull(igniteCfgFile, "Ignite config file");
        A.notNull(cacheName, "Cache name");

        this.evtBufSize = evtBufSize;
        this.evtBatchSize = evtBatchSize;

        Ignite ignite = IgniteSource.IgniteContext.getIgnite();
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

        Ignite ignite = IgniteSource.IgniteContext.getIgnite();

        if (rmtLsnrId != null)
            ignite.events(ignite.cluster().forCacheNodes(cacheName))
                    .stopRemoteListen(rmtLsnrId);

        rmtLsnrId = null;

        IgniteSource.IgniteContext.getIgnite().cache(cacheName).close();
    }

    /**
     * Transfers data from grid.
     *
     * @param ctx SourceContext.
     */
    @Override
    public void run(SourceContext<CacheEvent> ctx) {
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
            log.error("Error while processing OUT of " + cacheName, e);
        }
        return;
    }

    @Override
    public void cancel() {
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
    private static class IgniteContext {
        /** Constructor. */
        private IgniteContext() {
        }

        /** Instance holder. */
        private static class Holder {
            private static final Ignite IGNITE = Ignition.start(igniteCfgFile);
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        private static Ignite getIgnite() {
            return IgniteSource.IgniteContext.Holder.IGNITE;
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
     * Local listener buffering cache events to be further sent to Flink.
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
