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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.query.calcite.message.MarshalableMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class FragmentDescription implements MarshalableMessage {
    /** */
    @Order(0)
    private long fragmentId;

    /** */
    @Order(1)
    private FragmentMapping mapping;

    /** */
    @Order(2)
    private Map<Long, UUIDCollectionMessage> remoteSources0;

    /** */
    @Order(3)
    private ColocationGroup target;

    /** */
    private Map<Long, List<UUID>> remoteSources;

    /** */
    public FragmentDescription() {
    }

    /** */
    public FragmentDescription(long fragmentId, FragmentMapping mapping, ColocationGroup target,
        Map<Long, List<UUID>> remoteSources) {
        this.fragmentId = fragmentId;
        this.mapping = mapping;
        this.target = target;
        this.remoteSources = remoteSources;
    }

    /** */
    public long fragmentId() {
        return fragmentId;
    }

    /** */
    public void fragmentId(long fragmentId) {
        this.fragmentId = fragmentId;
    }

    /** */
    public List<UUID> nodeIds() {
        return mapping.nodeIds();
    }

    /** */
    public ColocationGroup target() {
        return target;
    }

    /** */
    public void target(ColocationGroup target) {
        this.target = target;
    }

    /** */
    public Map<Long, List<UUID>> remotes() {
        return remoteSources;
    }

    /** */
    public FragmentMapping mapping() {
        return mapping;
    }

    /** */
    public void mapping(FragmentMapping mapping) {
        this.mapping = mapping;
    }

    /** */
    public Map<Long, UUIDCollectionMessage> remoteSources0() {
        return remoteSources0;
    }

    /** */
    public void remoteSources0(Map<Long, UUIDCollectionMessage> remoteSources0) {
        this.remoteSources0 = remoteSources0;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.FRAGMENT_DESCRIPTION;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) {
        if (mapping != null)
            mapping.prepareMarshal(ctx);

        if (target != null) {
            target = target.explicitMapping();

            target.prepareMarshal(ctx);
        }

        if (remoteSources0 == null && remoteSources != null) {
            remoteSources0 = U.newHashMap(remoteSources.size());

            for (Map.Entry<Long, List<UUID>> e : remoteSources.entrySet())
                remoteSources0.put(e.getKey(), new UUIDCollectionMessage(e.getValue()));
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) {
        if (mapping != null)
            mapping.prepareUnmarshal(ctx);

        if (target != null)
            target.prepareUnmarshal(ctx);

        if (remoteSources == null && remoteSources0 != null) {
            remoteSources = U.newHashMap(remoteSources0.size());

            for (Map.Entry<Long, UUIDCollectionMessage> e : remoteSources0.entrySet())
                remoteSources.put(e.getKey(), new ArrayList<>(e.getValue().uuids()));
        }
    }
}
