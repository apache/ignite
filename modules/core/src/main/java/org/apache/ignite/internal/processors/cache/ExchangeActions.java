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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Cache change requests to execute when receive {@link DynamicCacheChangeBatch} event.
 */
public class ExchangeActions {
    /** */
    private Map<String, CacheActionData> cachesToStart;

    /** */
    private Map<String, CacheActionData> cachesToStop;

    /** */
    private Map<String, CacheActionData> cachesToClose;

    /**
     * @return New caches start requests.
     */
    public Collection<CacheActionData> cacheStartRequests() {
        return cachesToStart != null ? cachesToStart.values() : Collections.<CacheActionData>emptyList();
    }

    /**
     * @return Stop cache requests.
     */
    public Collection<CacheActionData> cacheStopRequests() {
        return cachesToStop != null ? cachesToStop.values() : Collections.<CacheActionData>emptyList();
    }

    /**
     * @return {@code True} if have start requests.
     */
    public boolean hasStart() {
        return !F.isEmpty(cachesToStart);
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if cache stop was requested.
     */
    public boolean cacheStopped(int cacheId) {
        if (cachesToStop != null) {
            for (CacheActionData cache : cachesToStop.values()) {
                if (cache.desc.cacheId() == cacheId)
                    return true;
            }
        }

        return false;
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if cache start was requested.
     */
    public boolean cacheStarted(int cacheId) {
        if (cachesToStart != null) {
            for (CacheActionData cache : cachesToStart.values()) {
                if (cache.desc.cacheId() == cacheId && !cache.request().clientStartOnly())
                    return true;
            }
        }

        return false;
    }

    /**
     * @param map Actions map.
     * @param req Request.
     * @param desc Cache descriptor.
     * @return Actions map.
     */
    private Map<String, CacheActionData> add(Map<String, CacheActionData> map,
        DynamicCacheChangeRequest req,
        DynamicCacheDescriptor desc) {
        assert req != null;
        assert desc != null;

        if (map == null)
            map = new LinkedHashMap<>();

        CacheActionData old = map.put(req.cacheName(), new CacheActionData(req, desc));

        assert old == null : old;

        return map;
    }

    /**
     * @param req Request.
     * @param desc Cache descriptor.
     */
    void addCacheToStart(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        assert req.start() : req;

        cachesToStart = add(cachesToStart, req, desc);
    }

    /**
     * @param req Request.
     * @param desc Cache descriptor.
     */
    void addCacheToStop(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        assert req.stop() : req;

        cachesToStop = add(cachesToStop, req, desc);
    }

    /**
     * @param req Request.
     * @param desc Cache descriptor.
     */
    void addCacheToClose(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        assert req.close() : req;

        cachesToClose = add(cachesToClose, req, desc);
    }

    /**
     * @param nodeId Local node ID.
     * @return Close cache requests.
     */
    List<DynamicCacheChangeRequest> cacheCloseRequests(UUID nodeId) {
        List<DynamicCacheChangeRequest> res = null;

        if (cachesToClose != null) {
            for (CacheActionData req : cachesToClose.values()) {
                if (nodeId.equals(req.req.initiatingNodeId())) {
                    if (res == null)
                        res = new ArrayList<>(cachesToClose.size());

                    res.add(req.req);
                }
            }
        }

        return res != null ? res : Collections.<DynamicCacheChangeRequest>emptyList();
    }

    /**
     * @return {@code True} if there are no cache change actions.
     */
    public boolean empty() {
        return F.isEmpty(cachesToStart) &&
            F.isEmpty(cachesToStop) &&
            F.isEmpty(cachesToClose);
    }

    /**
     * @param ctx Context.
     */
    public void completeRequestFutures(GridCacheSharedContext ctx, @Nullable Throwable err) {
        completeRequestFutures(cachesToStart, ctx, err);
        completeRequestFutures(cachesToStop, ctx, err);
        completeRequestFutures(cachesToClose, ctx, err);
    }

    /**
     * @param map Actions map.
     * @param ctx Context.
     */
    private void completeRequestFutures(Map<String, CacheActionData> map, GridCacheSharedContext ctx, @Nullable Throwable err) {
        if (map != null) {
            for (CacheActionData req : map.values())
                ctx.cache().completeStartFuture(req.req, err);
        }
    }

    /**
     *
     */
    public static class CacheActionData {
        /** */
        private final DynamicCacheChangeRequest req;

        /** */
        private final DynamicCacheDescriptor desc;

        /**
         * @param req Request.
         * @param desc Cache descriptor.
         */
        CacheActionData(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
            assert req != null;
            assert desc != null;

            this.req = req;
            this.desc = desc;
        }

        /**
         * @return Request.
         */
        public DynamicCacheChangeRequest request() {
            return req;
        }

        /**
         * @return Cache descriptor.
         */
        public DynamicCacheDescriptor descriptor() {
            return desc;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ExchangeActions [startCaches=" + (cachesToStart != null ? cachesToStart.keySet() : null) +
            ", stopCaches=" + (cachesToStop != null ? cachesToStop.keySet() : null) +
            ", closeCaches=" + (cachesToClose != null ? cachesToClose.keySet() : null)
            + ']';
    }
}
