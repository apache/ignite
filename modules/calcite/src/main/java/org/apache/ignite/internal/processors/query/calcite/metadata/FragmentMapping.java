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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class FragmentMapping implements CalciteMessage {
    /** */
    @Order(0)
    List<ColocationGroup> colocationGrps;

    /** */
    public FragmentMapping() {
    }

    /** */
    private FragmentMapping(ColocationGroup colocationGrp) {
        this(F.asList(colocationGrp));
    }

    /** */
    private FragmentMapping(List<ColocationGroup> colocationGrps) {
        this.colocationGrps = colocationGrps;
    }

    /** */
    public static FragmentMapping create() {
        return new FragmentMapping(new ColocationGroup());
    }

    /** */
    public static FragmentMapping create(UUID nodeId) {
        return new FragmentMapping(ColocationGroup.forNodes(Collections.singletonList(nodeId)));
    }

    /** */
    public static FragmentMapping create(long srcId) {
        return new FragmentMapping(ColocationGroup.forSourceId(srcId));
    }

    /** */
    public static FragmentMapping create(long srcId, ColocationGroup grp) {
        try {
            return new FragmentMapping(ColocationGroup.forSourceId(srcId).colocate(grp));
        }
        catch (ColocationMappingException e) {
            throw new AssertionError(e); // Cannot happen
        }
    }

    /** */
    public boolean colocated() {
        return colocationGrps.isEmpty() || colocationGrps.size() == 1;
    }

    /** */
    public FragmentMapping combine(FragmentMapping other) {
        return new FragmentMapping(Commons.combine(colocationGrps, other.colocationGrps));
    }

    /** */
    public FragmentMapping colocate(FragmentMapping other) throws ColocationMappingException {
        assert colocated() && other.colocated();

        ColocationGroup first = F.first(colocationGrps);
        ColocationGroup second = F.first(other.colocationGrps);

        if (first == null && second == null)
            return this;
        else if (first == null || second == null)
            return new FragmentMapping(U.firstNotNull(first, second));
        else
            return new FragmentMapping(first.colocate(second));
    }

    /** */
    public FragmentMapping local(UUID nodeId) throws ColocationMappingException {
        if (colocationGrps.isEmpty())
            return create(nodeId).colocate(this);

        return new FragmentMapping(Commons.transform(colocationGrps, c -> c.local(nodeId)));
    }

    /** */
    public List<UUID> nodeIds() {
        return colocationGrps.stream()
            .flatMap(g -> g.nodeIds().stream())
            .distinct().collect(Collectors.toList());
    }

    /** */
    public List<ColocationGroup> colocationGroups() {
        return colocationGrps == null ? Collections.emptyList() : Collections.unmodifiableList(colocationGrps);
    }

    /** */
    public void colocationGroups(List<ColocationGroup> colocationGrps) {
        this.colocationGrps = colocationGrps;
    }

    /** */
    public FragmentMapping finalizeMapping(Supplier<List<UUID>> nodesSrc) {
        if (colocationGrps.isEmpty())
            return this;

        List<ColocationGroup> colocationGrps = this.colocationGrps;

        colocationGrps = Commons.transform(colocationGrps, ColocationGroup::finalizeMapping);
        List<UUID> nodes = nodeIds(), nodes0 = nodes.isEmpty() ? nodesSrc.get() : nodes;
        colocationGrps = Commons.transform(colocationGrps, g -> g.mapToNodes(nodes0));

        return new FragmentMapping(colocationGrps);
    }

    /** */
    public FragmentMapping filterByPartitions(int[] parts) throws ColocationMappingException {
        List<ColocationGroup> colocationGrps = this.colocationGrps;

        if (!F.isEmpty(parts) && colocationGrps.size() > 1)
            throw new ColocationMappingException("Execution of non-collocated query with partition parameter is not possible");

        colocationGrps = Commons.transform(colocationGrps, g -> g.filterByPartitions(parts));

        return new FragmentMapping(colocationGrps);
    }

    /** */
    public @NotNull ColocationGroup findGroup(long srcId) {
        List<ColocationGroup> grps = colocationGrps.stream()
            .filter(c -> c.belongs(srcId))
            .collect(Collectors.toList());

        if (grps.isEmpty())
            throw new IllegalStateException("Failed to find group with given id. [sourceId=" + srcId + "]");
        else if (grps.size() > 1)
            throw new IllegalStateException("Multiple groups with the same id found. [sourceId=" + srcId + "]");

        return F.first(grps);
    }

    /** Create fragment mapping with explicit mapping for groups by source ids. */
    public FragmentMapping explicitMapping(Set<Long> srcIds) {
        Set<ColocationGroup> explicitMappingGrps = U.newIdentityHashSet();

        srcIds.forEach(srcId -> explicitMappingGrps.add(findGroup(srcId)));

        return new FragmentMapping(Commons.transform(colocationGrps,
            g -> explicitMappingGrps.contains(g) ? g.explicitMapping() : g));
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.FRAGMENT_MAPPING;
    }
}
