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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;

/**
 * Mapper node results.
 */
class MapNodeResults {
    /** */
    private final ConcurrentMap<MapRequestKey, MapQueryResults> res = new ConcurrentHashMap<>();

    /** Cancel state for update requests. */
    private final ConcurrentMap<Long, GridQueryCancel> updCancels = new ConcurrentHashMap<>();

    /** */
    private final GridBoundedConcurrentLinkedHashMap<Long, Boolean> qryHist =
        new GridBoundedConcurrentLinkedHashMap<>(1024, 1024, 0.75f, 64, PER_SEGMENT_Q);

    /** Node ID. */
    private final UUID nodeId;

    /**
     * Constructor.
     *
     * @param nodeId Node ID.
     */
    public MapNodeResults(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @param reqId Query Request ID.
     * @return {@code False} if query was already cancelled.
     */
    boolean cancelled(long reqId) {
        return qryHist.get(reqId) != null;
    }

    /**
     * @param reqId Query Request ID.
     * @return {@code True} if cancelled.
     */
    boolean onCancel(long reqId) {
        Boolean old = qryHist.putIfAbsent(reqId, Boolean.FALSE);

        return old == null;
    }

    /**
     * @param reqId Query Request ID.
     * @param segmentId Index segment ID.
     * @return query partial results.
     */
    public MapQueryResults get(long reqId, int segmentId) {
        return res.get(new MapRequestKey(nodeId, reqId, segmentId));
    }

    /**
     * Cancel all thread of given request.
     * @param reqId Request ID.
     */
    public void cancelRequest(long reqId) {
        for (MapRequestKey key : res.keySet()) {
            if (key.requestId() == reqId) {
                MapQueryResults removed = res.remove(key);

                if (removed != null)
                    removed.cancel();
            }
        }

        // Cancel update request
        GridQueryCancel updCancel = updCancels.remove(reqId);

        if (updCancel != null)
            updCancel.cancel();
    }

    /**
     * @param reqId Query Request ID.
     * @param segmentId Index segment ID.
     * @param qr Query Results.
     * @return {@code True} if removed.
     */
    public boolean remove(long reqId, int segmentId, MapQueryResults qr) {
        return res.remove(new MapRequestKey(nodeId, reqId, segmentId), qr);
    }

    /**
     * @param reqId Query Request ID.
     * @param segmentId Index segment ID.
     * @param qr Query Results.
     * @return previous value.
     */
    public MapQueryResults put(long reqId, int segmentId, MapQueryResults qr) {
        return res.put(new MapRequestKey(nodeId, reqId, segmentId), qr);
    }

    /**
     * @param reqId Request id.
     * @return Cancel state.
     */
    public GridQueryCancel putUpdate(long reqId) {
        GridQueryCancel cancel = new GridQueryCancel();

        updCancels.put(reqId, cancel);

        return cancel;
    }

    /**
     * @param reqId Request id.
     */
    public void removeUpdate(long reqId) {
        updCancels.remove(reqId);
    }

    /**
     * Cancel all node queries.
     */
    public void cancelAll() {
        for (MapQueryResults ress : res.values())
            ress.cancel();

        // Cancel update requests
        for (GridQueryCancel upd: updCancels.values())
            upd.cancel();
    }
}
