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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToIntFunction;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
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
    @Override final public int hashCode() {
        return Objects.hashCode(name());
    }

    /** {@inheritDoc} */
    @Override final public boolean equals(Object obj) {
        if (obj instanceof DistributionFunction)
            //noinspection StringEquality
            return name() == ((DistributionFunction) obj).name();

        return false;
    }

    /** {@inheritDoc} */
    @Override final public String toString() {
        return name();
    }

    /** */
    public static final class Any extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new Any();

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
    public static final class Broadcast extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new Broadcast();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.BROADCAST_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            List<UUID> nodes = m.nodes();

            return r -> nodes;
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class Random extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new Random();

        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.RANDOM_DISTRIBUTED;
        }

        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            List<UUID> nodes = m.nodes();

            return r -> Collections.singletonList(nodes.get(ThreadLocalRandom.current().nextInt(nodes.size())));
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class Singleton extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new Singleton();

        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.SINGLETON;
        }

        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            List<UUID> nodes = Collections.singletonList(Objects.requireNonNull(F.first(m.nodes())));

            return r -> nodes;
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class Hash extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new Hash();

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.assignments());

            int[] fields = k.toIntArray();

            ToIntFunction<Object> hashFun = r -> {
                Object[] row = (Object[]) r;

                if (row == null)
                    return 0;

                int hash = 1;

                for (int i : fields)
                    hash = 31 * hash + (row[i] == null ? 0 : row[i].hashCode());

                return hash;
            };

            List<List<UUID>> assignments = m.assignments();

            if (U.assertionsEnabled()) {
                for (List<UUID> assignment : assignments) {
                    assert F.isEmpty(assignment) || assignment.size() == 1;
                }
            }

            return r -> assignments.get(hashFun.applyAsInt(r) % assignments.size());
        }

        /** */
        private Object readResolve() throws ObjectStreamException {
            return INSTANCE;
        }
    }

    /** */
    public static final class Affinity extends DistributionFunction {
        private final int cacheId;
        private final Object key;

        public Affinity(int cacheId, Object key) {
            this.cacheId = cacheId;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public DestinationFunction toDestination(PlannerContext ctx, NodesMapping mapping, ImmutableIntList keys) {
            assert keys.size() == 1 && mapping != null && !F.isEmpty(mapping.assignments());

            List<List<UUID>> assignments = mapping.assignments();

            if (U.assertionsEnabled()) {
                for (List<UUID> assignment : assignments) {
                    assert F.isEmpty(assignment) || assignment.size() == 1;
                }
            }

            ToIntFunction<Object> rowToPart = ctx.kernalContext()
                .cache().context().cacheContext(cacheId).affinity()::partition;

            return row -> assignments.get(rowToPart.applyAsInt(((Object[]) row)[keys.getInt(0)]));
        }

        /** {@inheritDoc} */
        @Override protected String name0() {
            return "affinity[" + key + "]";
        }
    }
}
