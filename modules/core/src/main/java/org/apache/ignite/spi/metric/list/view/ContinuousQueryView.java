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

package org.apache.ignite.spi.metric.list.view;

import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.LocalRoutineInfo;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.RemoteRoutineInfo;
import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;

/**
 * Continuous query representation for a {@link MonitoringList}.
 */
public class ContinuousQueryView implements MonitoringRow<UUID> {
    /** Remote CQ info. */
    private final RemoteRoutineInfo rmtQry;

    /** Local CQ info. */
    private final LocalRoutineInfo locQry;

    /** CQ Handler. */
    private final GridContinuousHandler hnd;

    /** CQ id */
    private final UUID routineId;

    /**
     * @param rmtQry Remote CQ info.
     * @param routineId CQ id.
     */
    public ContinuousQueryView(RemoteRoutineInfo rmtQry, UUID routineId) {
        this.locQry = null;
        this.rmtQry = rmtQry;
        this.hnd = rmtQry.handler();
        this.routineId = routineId;
    }

    /**
     * @param locQry Local CQ info.
     * @param routineId CQ id.
     */
    public ContinuousQueryView(LocalRoutineInfo locQry, UUID routineId) {
        this.locQry = locQry;
        this.rmtQry = null;
        this.hnd = locQry.handler();
        this.routineId = routineId;
    }

    /** {@inheritDoc} */
    @Override public UUID monitoringRowId() {
        return routineId;
    }

    /** @return CQ id. */
    public UUID routineId() {
        return routineId;
    }

    /** @return Node id. */
    public UUID nodeId() {
        if (locQry != null)
            return locQry.nodeId();

        return rmtQry.nodeId();
    }

    /** @return Cache name. */
    @Order
    public String cacheName() {
        return hnd.cacheName();
    }

    /** @return Topic for CQ messages. */
    public String topic() {
        return String.valueOf(hnd.orderedTopic());
    }

    /** @return Buffer size. */
    public int bufferSize() {
        if (locQry != null)
            return locQry.bufferSize();

        return rmtQry.bufferSize();
    }

    /** @return Notify interval. */
    public long interval() {
        if (locQry != null)
            return locQry.interval();

        return rmtQry.interval();
    }

    /** @return Auto unsubscribe flag value. */
    public boolean autoUnsubscribe() {
        if (locQry != null)
            return locQry.autoUnsubscribe();

        return rmtQry.autoUnsubscribe();
    }

    /** @return {@code True} if CQ registered to receive events. */
    public boolean isEvents() {
        return hnd.isEvents();
    }

    /** @return {@code True} if CQ registered for messaging. */
    public boolean isMessaging() {
        return hnd.isMessaging();
    }

    /** @return {@code True} if regular CQ. */
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

    /** @return {@code True} if CQ should receive notification for existing entries. */
    public boolean notifyExisting() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        return hnd0 != null && hnd0.notifyExisting();
    }

    /** @return {@code True} if old value required for listener. */
    public boolean oldValueRequired() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null)
            return false;

        return hnd0.oldValueRequired();
    }

    /** @return Last send time. */
    @Order(5)
    public long lastSendTime() {
        if (locQry != null)
            return -1;

        return rmtQry.lastSendTime();
    }

    /** @return Delayed register flag. */
    public boolean delayedRegister() {
        if (locQry != null)
            return false;

        return rmtQry.delayedRegister();
    }

    /** 
     * @return String representation of local listener 
     * @see ContinuousQuery#setLocalListener(CacheEntryUpdatedListener) 
     */
    @Order(1)
    public String localListener() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.localListener() == null)
            return null;

        return hnd0.localListener().getClass().getName();
    }

    /**
     * @return String representation of remote filter
     * @see ContinuousQuery#setRemoteFilter(CacheEntryEventSerializableFilter)
     * @see ContinuousQuery#setRemoteFilterFactory(Factory)
     */
    @Order(2)
    public String remoteFilter() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        try {
            if (hnd0 == null || hnd0.getEventFilter() == null)
                return null;

            return hnd0.getEventFilter().getClass().getName();
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

        return hnd0.getTransformer().getClass().getName();
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

        return hnd0.localTransformedEventListener().getClass().getName();
    }

    /** */
    private CacheContinuousQueryHandler cacheHandler() {
        if (!(hnd instanceof CacheContinuousQueryHandler))
            return null;

        return (CacheContinuousQueryHandler)hnd;
    }
}
