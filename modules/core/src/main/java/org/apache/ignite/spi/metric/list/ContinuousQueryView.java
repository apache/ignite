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

package org.apache.ignite.spi.metric.list;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.LocalRoutineInfo;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.RemoteRoutineInfo;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;

/** */
public class ContinuousQueryView implements MonitoringRow<UUID> {
    /** */
    private final RemoteRoutineInfo rmtQry;

    /** */
    private final LocalRoutineInfo locQry;

    /** */
    private final GridContinuousHandler hnd;

    /** */
    public ContinuousQueryView(RemoteRoutineInfo rmtQry) {
        this.locQry = null;
        this.rmtQry = rmtQry;
        this.hnd = rmtQry.handler();
    }

    /** */
    public ContinuousQueryView(LocalRoutineInfo locQry) {
        this.locQry = locQry;
        this.rmtQry = null;
        this.hnd = locQry.handler();
    }

    /** */
    public UUID nodeId() {
        if (locQry != null)
            return locQry.nodeId();

        return rmtQry.nodeId();
    }

    /** */
    public String cacheName() {
        return hnd.cacheName();
    }

    /** */
    public String topic() {
        return String.valueOf(hnd.orderedTopic());
    }

    /** */
    public int bufferSize() {
        if (locQry != null)
            return locQry.bufferSize();

        return rmtQry.bufferSize();
    }

    /** */
    public long interval() {
        if (locQry != null)
            return locQry.interval();

        return rmtQry.interval();
    }

    /** */
    public boolean autoUnsubscribe() {
        if (locQry != null)
            return locQry.autoUnsubscribe();

        return rmtQry.autoUnsubscribe();
    }

    /** */
    public boolean isEvents() {
        return hnd.isEvents();
    }

    /** */
    public boolean isMessaging() {
        return hnd.isMessaging();
    }

    /** */
    public boolean isQuery() {
        return hnd.isQuery();
    }

    /** */
    public boolean keepBinary() {
        return hnd.keepBinary();
    }

    /** */
    public boolean notifyExisting() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        return hnd0 != null && hnd0.notifyExisting();
    }

    public boolean oldValueRequired() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null)
            return false;

        return hnd0.oldValueRequired();
    }

    /** */
    public long lastSendTime() {
        if (locQry != null)
            return -1;

        return rmtQry.lastSendTime();
    }

    /** */
    public boolean delayedRegister() {
        if (locQry != null)
            return false;

        return rmtQry.delayedRegister();
    }

    /** */
    public String localListener() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.localListener() == null)
            return null;

        return hnd0.localListener().getClass().getName();
    }

    /** */
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

    /** */
    public String remoteTransformer() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.getTransformer() == null)
            return null;

        return hnd0.getTransformer().getClass().getName();
    }

    /** */
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
