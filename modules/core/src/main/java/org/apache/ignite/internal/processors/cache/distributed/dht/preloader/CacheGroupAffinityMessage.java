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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Information about affinity assignment.
 */
public class CacheGroupAffinityMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectCollection(GridLongList.class)
    private List<GridLongList> assigns;

    /** */
    @GridDirectCollection(GridLongList.class)
    private List<GridLongList> idealAssigns;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = GridLongList.class)
    private Map<Integer, GridLongList> assignsDiff;

    /**
     *
     */
    public CacheGroupAffinityMessage() {
        // No-op.
    }

    /**
     * @param assign0 Assignment.
     * @param idealAssign0 Ideal assignment.
     * @param assignDiff0 Difference with ideal affinity assignment.
     */
    private CacheGroupAffinityMessage(List<List<ClusterNode>> assign0,
        List<List<ClusterNode>> idealAssign0,
        Map<Integer, List<Long>> assignDiff0) {
        if (assign0 != null)
            assigns = createAssigns(assign0);

        if (idealAssign0 != null)
            idealAssigns = createAssigns(idealAssign0);

        if (assignDiff0 != null) {
            assignsDiff = U.newHashMap(assignDiff0.size());

            for (Map.Entry<Integer, List<Long>> e : assignDiff0.entrySet()) {
                List<Long> orders = e.getValue();

                GridLongList l = new GridLongList(orders.size());

                for (int n = 0; n < orders.size(); n++)
                    l.add(orders.get(n));

                assignsDiff.put(e.getKey(), l);
            }
        }
    }

    private List<GridLongList> createAssigns(List<List<ClusterNode>> assign0) {
        if (assign0 != null) {
            List<GridLongList> assigns = new ArrayList<>(assign0.size());

            for (int i = 0; i < assign0.size(); i++) {
                List<ClusterNode> nodes = assign0.get(i);

                GridLongList l = new GridLongList(nodes.size());

                for (int n = 0; n < nodes.size(); n++)
                    l.add(nodes.get(n).order());

                assigns.add(l);
            }

            return assigns;
        }

        return null;
    }

    /**
     * @param affDiff Affinity diff.
     * @return Affinity diff messages.
     */
    public static Map<Integer, CacheGroupAffinityMessage> createAffinityDiffMessages(
        Map<Integer, Map<Integer, List<Long>>> affDiff) {
        if (F.isEmpty(affDiff))
            return null;

        Map<Integer, CacheGroupAffinityMessage> map = U.newHashMap(affDiff.size());

        for (Map.Entry<Integer, Map<Integer, List<Long>>> e : affDiff.entrySet())
            map.put(e.getKey(), new CacheGroupAffinityMessage(null, null, e.getValue()));

        return map;
    }

    /**
     * @param cctx Context.
     * @param topVer Topology version.
     * @param affReq Cache group IDs.
     * @param cachesAff Optional already prepared affinity.
     * @return Affinity.
     */
    static Map<Integer, CacheGroupAffinityMessage> createAffinityMessages(
        GridCacheSharedContext cctx,
        AffinityTopologyVersion topVer,
        Collection<Integer> affReq,
        @Nullable Map<Integer, CacheGroupAffinityMessage> cachesAff
    ) {
        assert !F.isEmpty(affReq) : affReq;

        if (cachesAff == null)
            cachesAff = U.newHashMap(affReq.size());

        for (Integer grpId : affReq) {
            if (!cachesAff.containsKey(grpId)) {
                GridAffinityAssignmentCache aff = cctx.affinity().groupAffinity(grpId);

                // If no coordinator group holder on the node, try fetch affinity from existing cache group.
                if (aff == null) {
                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    assert grp != null : "No cache group holder or cache group to create AffinityMessage"
                        + ". Requested group id: " + grpId
                        + ". Topology version: " + topVer;

                    aff = grp.affinity();
                }

                List<List<ClusterNode>> assign = aff.readyAssignments(topVer);

                CacheGroupAffinityMessage msg = new CacheGroupAffinityMessage(assign,
                    aff.centralizedAffinityFunction() ? aff.idealAssignment() : null,
                    null);

                cachesAff.put(grpId, msg);
            }
        }

        return cachesAff;
    }

    /**
     * @param assign Nodes orders.
     * @param nodesByOrder Nodes by order cache.
     * @param discoCache Discovery data cache.
     * @return Nodes list.
     */
    public static List<ClusterNode> toNodes(GridLongList assign, Map<Long, ClusterNode> nodesByOrder, DiscoCache discoCache) {
        List<ClusterNode> assign0 = new ArrayList<>(assign.size());

        for (int n = 0; n < assign.size(); n++) {
            long order = assign.get(n);

            ClusterNode affNode = nodesByOrder.get(order);

            if (affNode == null) {
                affNode = discoCache.serverNodeByOrder(order);

                assert affNode != null : "Failed to find node by order [order=" + order +
                    ", topVer=" + discoCache.version() + ']';

                nodesByOrder.put(order, affNode);
            }

            assign0.add(affNode);
        }

        return assign0;
    }

    /**
     * @param nodesByOrder Nodes by order cache.
     * @param discoCache Discovery data cache.
     * @return Nodes list.
     */
    @Nullable public List<List<ClusterNode>> createIdealAssignments(Map<Long, ClusterNode> nodesByOrder,
        DiscoCache discoCache) {
        if (idealAssigns == null)
            return null;

        return createAssignments(idealAssigns, nodesByOrder, discoCache);
    }

    /**
     * @param nodesByOrder Nodes by order cache.
     * @param discoCache Discovery data cache.
     * @return Assignments.
     */
    public List<List<ClusterNode>> createAssignments(Map<Long, ClusterNode> nodesByOrder, DiscoCache discoCache) {
        return createAssignments(assigns, nodesByOrder, discoCache);
    }

    /**
     * @param assigns Nodes orders.
     * @param nodesByOrder Nodes by order cache.
     * @param discoCache Discovery data cache.
     * @return Nodes list.
     */
    private List<List<ClusterNode>> createAssignments(List<GridLongList> assigns,
        Map<Long, ClusterNode> nodesByOrder,
        DiscoCache discoCache) {
        List<List<ClusterNode>> assignments0 = new ArrayList<>(assigns.size());

        for (int p = 0; p < assigns.size(); p++) {
            GridLongList assign = assigns.get(p);

            assignments0.add(toNodes(assign, nodesByOrder, discoCache));
        }

        return assignments0;
    }

    /**
     * @return Difference with ideal affinity assignment.
     */
    public Map<Integer, GridLongList> assignmentsDiff() {
        return assignsDiff;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection("assigns", assigns, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap("assignsDiff", assignsDiff, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("idealAssigns", idealAssigns, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                assigns = reader.readCollection("assigns", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                assignsDiff = reader.readMap("assignsDiff", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                idealAssigns = reader.readCollection("idealAssigns", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheGroupAffinityMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 128;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheGroupAffinityMessage.class, this);
    }
}
