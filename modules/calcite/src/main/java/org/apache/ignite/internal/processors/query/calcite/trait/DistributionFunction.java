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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.function.ToIntFunction;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Distribution function.
 */
public abstract class DistributionFunction implements Serializable {
    /** */
    private transient String name;

    /** */
    private DistributionFunction(){}

    /**
     * @return Distribution function type.
     */
    public abstract RelDistribution.Type type();

    /**
     * Creates a destination function based on this function algorithm, given nodes mapping and given distribution keys.
     *
     * @param ctx Planner context.
     * @param mapping Target mapping.
     * @param keys Distribution keys.
     * @return Destination function.
     */
    public abstract DestinationFunction toDestination(PlannerContext ctx, NodesMapping mapping, ImmutableIntList keys);

    /**
     * @return Function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    public final String name(){
        if (name != null)
            return name;

        return name = name0().intern();
    }

    /**
     * @return Function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    protected String name0() {
        return type().shortName;
    }

    /** {@inheritDoc} */
    @Override public final int hashCode() {
        return Objects.hashCode(name());
    }

    /** {@inheritDoc} */
    @Override public final boolean equals(Object obj) {
        if (obj instanceof DistributionFunction)
            //noinspection StringEquality
            return name() == ((DistributionFunction) obj).name();

        return false;
    }

    /** {@inheritDoc} */
    @Override public final String toString() {
        return name();
    }

    /** */
    public static final class AnyDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new AnyDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.ANY;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            throw new AssertionError();
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class BroadcastDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new BroadcastDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.BROADCAST_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.nodes());

            List<UUID> nodes = m.nodes();

            return new AllNodesFunction(nodes);
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class RandomDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new RandomDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.RANDOM_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.nodes());

            List<UUID> nodes = m.nodes();

            return new RandomNodeFunction(nodes);
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }

    }

    /** */
    public static final class SingletonDistribution extends DistributionFunction {
        /** */
        public static final DistributionFunction INSTANCE = new SingletonDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.SINGLETON;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            assert m != null && m.nodes() != null && m.nodes().size() == 1;

            List<UUID> nodes = Collections.singletonList(Objects.requireNonNull(F.first(m.nodes())));

            return new AllNodesFunction(nodes);
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }

    }

    /** */
    public static final class HashDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new HashDistribution();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.assignments());

            List<List<UUID>> assignments = m.assignments();

            if (U.assertionsEnabled()) {
                for (List<UUID> assignment : assignments) {
                    assert F.isEmpty(assignment) || assignment.size() == 1;
                }
            }

            int[] fields = k.toIntArray();

            ToIntFunction<Object> rowToPart = r -> {
                Object[] row = (Object[]) r;

                if (row == null)
                    return 0;

                int hash = 1;

                for (int i : fields)
                    hash = 31 * hash + (row[i] == null ? 0 : row[i].hashCode());

                return hash;
            };

            List<UUID> nodes = m.nodes();

            return new PartitionFunction(nodes, assignments, rowToPart);
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class AffinityDistribution extends DistributionFunction {
        /** */
        private final int cacheId;

        /** */
        private final Object key;

        /**
         * @param cacheId Cache ID.
         * @param key Affinity identity key.
         */
        public AffinityDistribution(int cacheId, Object key) {
            this.cacheId = cacheId;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.assignments()) && k.size() == 1;

            List<List<UUID>> assignments = m.assignments();

            if (U.assertionsEnabled()) {
                for (List<UUID> assignment : assignments) {
                    assert F.isEmpty(assignment) || assignment.size() == 1;
                }
            }

            AffinityFunction affinity = ctx.affinityFunction(cacheId);

            int field = k.getInt(0);

            ToIntFunction<Object> rowToPart = row -> affinity.partition(((Object[]) row)[field]);

            List<UUID> nodes = m.nodes();

            return new PartitionFunction(nodes, assignments, rowToPart);
        }

        /** {@inheritDoc} */
        @Override protected String name0() {
            return "affinity[" + key + "]";
        }
    }

    /** */
    private static class PartitionFunction implements DestinationFunction {
        /** */
        private final List<UUID> nodes;

        /** */
        private final List<List<UUID>> assignments;

        /** */
        private final ToIntFunction<Object> partFun;

        /** */
        private PartitionFunction(List<UUID> nodes, List<List<UUID>> assignments, ToIntFunction<Object> partFun) {
            this.nodes = nodes;
            this.assignments = assignments;
            this.partFun = partFun;
        }

        /** {@inheritDoc} */
        @Override public List<UUID> destination(Object row) {
            return assignments.get(partFun.applyAsInt(row) % assignments.size());
        }

        /** {@inheritDoc} */
        @Override public List<UUID> targets() {
            return nodes;
        }
    }

    /** */
    private static class AllNodesFunction implements DestinationFunction {
        /** */
        private final List<UUID> nodes;

        /** */
        private AllNodesFunction(List<UUID> nodes) {
            this.nodes = nodes;
        }

        /** {@inheritDoc} */
        @Override public List<UUID> destination(Object row) {
            return nodes;
        }

        /** {@inheritDoc} */
        @Override public List<UUID> targets() {
            return nodes;
        }
    }

    /** */
    private static class RandomNodeFunction implements DestinationFunction {
        /** */
        private final Random random;

        /** */
        private final List<UUID> nodes;

        /** */
        private RandomNodeFunction(List<UUID> nodes) {
            this.nodes = nodes;

            random = new Random();
        }

        /** {@inheritDoc} */
        @Override public List<UUID> destination(Object row) {
            return Collections.singletonList(nodes.get(random.nextInt(nodes.size())));
        }

        /** {@inheritDoc} */
        @Override public List<UUID> targets() {
            return nodes;
        }
    }
}
