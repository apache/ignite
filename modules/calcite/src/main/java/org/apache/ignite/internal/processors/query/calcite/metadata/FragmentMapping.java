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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.query.calcite.message.MarshalableMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MarshallingContext;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class FragmentMapping implements MarshalableMessage {
    /** */
    @GridDirectCollection(ColocationGroup.class)
    private List<ColocationGroup> colocationGroups;

    /** */
    public FragmentMapping() {
    }

    /** */
    private FragmentMapping(ColocationGroup colocationGroup) {
        this(F.asList(colocationGroup));
    }

    /** */
    private FragmentMapping(List<ColocationGroup> colocationGroups) {
        this.colocationGroups = colocationGroups;
    }

    /** */
    public static FragmentMapping create() {
        return new FragmentMapping(Collections.emptyList());
    }

    /** */
    public static FragmentMapping create(UUID nodeId) {
        return new FragmentMapping(ColocationGroup.forNodes(Collections.singletonList(nodeId)));
    }

    /** */
    public static FragmentMapping create(long sourceId) {
        return new FragmentMapping(ColocationGroup.forSourceId(sourceId));
    }

    /** */
    public static FragmentMapping create(long sourceId, ColocationGroup group) {
        try {
            return new FragmentMapping(ColocationGroup.forSourceId(sourceId).colocate(group));
        }
        catch (ColocationMappingException e) {
            throw new AssertionError(e); // Cannot happen
        }
    }

    /** */
    public boolean colocated() {
        return colocationGroups.isEmpty() || colocationGroups.size() == 1;
    }

    /** */
    public FragmentMapping prune(IgniteRel rel) {
        if (colocationGroups.size() != 1)
            return this;

        return new FragmentMapping(F.first(colocationGroups).prune(rel));
    }

    /** */
    public FragmentMapping combine(FragmentMapping other) {
        return new FragmentMapping(Commons.combine(colocationGroups, other.colocationGroups));
    }

    /** */
    public FragmentMapping colocate(FragmentMapping other) throws ColocationMappingException {
        assert colocated() && other.colocated();

        ColocationGroup first = F.first(colocationGroups);
        ColocationGroup second = F.first(other.colocationGroups);

        if (first == null && second == null)
            return this;
        else if (first == null || second == null)
            return new FragmentMapping(U.firstNotNull(first, second));
        else
            return new FragmentMapping(first.colocate(second));
    }

    /** */
    public List<UUID> nodeIds() {
        return colocationGroups.stream()
            .flatMap(g -> g.nodeIds().stream())
            .distinct().collect(Collectors.toList());
    }

    /** */
    public FragmentMapping finalize(Supplier<List<UUID>> nodesSource) {
        if (colocationGroups.isEmpty())
            return this;

        List<ColocationGroup> colocationGroups = this.colocationGroups;

        colocationGroups = Commons.transform(colocationGroups, ColocationGroup::finalaze);
        List<UUID> nodes = nodeIds(), nodes0 = nodes.isEmpty() ? nodesSource.get() : nodes;
        colocationGroups = Commons.transform(colocationGroups, g -> g.mapToNodes(nodes0));

        return new FragmentMapping(colocationGroups);
    }

    /** */
    public @NotNull ColocationGroup findGroup(long sourceId) {
        List<ColocationGroup> groups = colocationGroups.stream()
            .filter(c -> c.belongs(sourceId))
            .collect(Collectors.toList());

        if (groups.isEmpty())
            throw new IllegalStateException("Failed to find group with given id. [sourceId=" + sourceId + "]");
        else if (groups.size() > 1)
            throw new IllegalStateException("Multiple groups with the same id found. [sourceId=" + sourceId + "]");

        return F.first(groups);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(MarshallingContext ctx) {
        colocationGroups.forEach(g -> g.prepareMarshal(ctx));
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(MarshallingContext ctx) {
        colocationGroups.forEach(g -> g.prepareUnmarshal(ctx));
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.FRAGMENT_MAPPING;
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
                if (!writer.writeCollection("colocationGroups", colocationGroups, MessageCollectionItemType.MSG))
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
                colocationGroups = reader.readCollection("colocationGroups", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(FragmentMapping.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
