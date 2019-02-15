/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
