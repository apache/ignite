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

package org.apache.ignite.spi.systemview.view;

import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.RoutineInfo;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Continuous query representation for a {@link SystemView}.
 */
public class ContinuousQueryView {
    /** Routine info. */
    private final RoutineInfo qry;

    /** Continuous query handler. */
    private final GridContinuousHandler hnd;

    /** Routine id. */
    private final UUID routineId;

    /**
     * @param routineId Routine id.
     * @param qry Query info.
     */
    public ContinuousQueryView(UUID routineId, RoutineInfo qry) {
        this.qry = qry;
        this.hnd = qry.handler();
        this.routineId = routineId;
    }

    /** @return Continuous query id. */
    public UUID routineId() {
        return routineId;
    }

    /** @return Node id. */
    public UUID nodeId() {
        return qry.nodeId();
    }

    /** @return Cache name. */
    @Order
    public String cacheName() {
        return hnd.cacheName();
    }

    /** @return Topic for continuous query messages. */
    public String topic() {
        return toStringSafe(hnd.orderedTopic());
    }

    /** @return Buffer size. */
    public int bufferSize() {
        return qry.bufferSize();
    }

    /** @return Notify interval. */
    public long interval() {
        return qry.interval();
    }

    /** @return Auto unsubscribe flag value. */
    public boolean autoUnsubscribe() {
        return qry.autoUnsubscribe();
    }

    /** @return {@code True} if continuous query registered to receive events. */
    public boolean isEvents() {
        return hnd.isEvents();
    }

    /** @return {@code True} if continuous query registered for messaging. */
    public boolean isMessaging() {
        return hnd.isMessaging();
    }

    /** @return {@code True} if regular continuous query. */
    public boolean isQuery() {
        return hnd.isQuery();
    }

    /**
     * @return {@code True} if {@code keepBinary} mode enabled.
     * @see IgniteCache#withKeepBinary()
     */
    public boolean keepBinary() {
        return hnd.keepBinary();
    }

    /** @return {@code True} if continuous query should receive notification for existing entries. */
    public boolean notifyExisting() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        return hnd0 != null && hnd0.notifyExisting();
    }

    /** @return {@code True} if old value required for listener. */
    public boolean oldValueRequired() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        return hnd0 != null && hnd0.oldValueRequired();
    }

    /** @return Last send time. */
    @Order(5)
    public long lastSendTime() {
        return qry.lastSendTime();
    }

    /** @return Delayed register flag. */
    public boolean delayedRegister() {
        return qry.delayedRegister();
    }

    /**
     * @return Class name of the local transformed listener.
     * @see ContinuousQuery#setLocalListener(CacheEntryUpdatedListener)
     */
    @Order(1)
    public String localListener() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.localListener() == null)
            return null;

        return toStringSafe(hnd0.localListener());
    }

    /**
     * @return String representation of remote filter.
     * @see ContinuousQuery#setRemoteFilter(CacheEntryEventSerializableFilter)
     * @see ContinuousQuery#setRemoteFilterFactory(Factory)
     */
    @Order(2)
    public String remoteFilter() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        try {
            if (hnd0 == null || hnd0.getEventFilter() == null)
                return null;

            return toStringSafe(hnd0.getEventFilter());
        }
        catch (IgniteCheckedException e) {
            return null;
        }
    }

    /**
     * @return String representation of remote transformer.
     * @see ContinuousQueryWithTransformer
     * @see ContinuousQueryWithTransformer#setRemoteTransformerFactory(Factory)
     */
    @Order(3)
    public String remoteTransformer() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.getTransformer() == null)
            return null;

        return toStringSafe(hnd0.getTransformer());
    }

    /**
     * @return String representation of local transformed listener.
     * @see ContinuousQueryWithTransformer
     * @see ContinuousQueryWithTransformer#setLocalListener(ContinuousQueryWithTransformer.EventListener)
     */
    @Order(4)
    public String localTransformedListener() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.localTransformedEventListener() == null)
            return null;

        return toStringSafe(hnd0.localTransformedEventListener());
    }

    /** */
    private CacheContinuousQueryHandler cacheHandler() {
        if (!(hnd instanceof CacheContinuousQueryHandler))
            return null;

        return (CacheContinuousQueryHandler)hnd;
    }
}
