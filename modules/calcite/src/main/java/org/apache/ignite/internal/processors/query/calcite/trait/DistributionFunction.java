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
import java.util.function.ToIntFunction;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
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
     * Creates a destination based on this function algorithm, given nodes mapping and given distribution keys.
     *
     * @param partitionService Affinity function source.
     * @param mapping Target mapping.
     * @param keys Distribution keys.
     * @return Destination function.
     */
    public abstract Destination destination(PartitionService partitionService, NodesMapping mapping, ImmutableIntList keys);

    /**
     * Creates a partition.
     *
     * @param partitionService Affinity function source.
     * @param partitionsCount Expected partitions count.
     * @param keys Distribution keys.
     * @return Partition function.
     */
    public ToIntFunction<Object> partitionFunction(PartitionService partitionService, int partitionsCount, ImmutableIntList keys) {
        throw new UnsupportedOperationException();
    }

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
        @Override public Destination destination(PartitionService partitionService, NodesMapping m, ImmutableIntList k) {
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
        @Override public Destination destination(PartitionService partitionService, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.nodes());

            return new AllNodes(m.nodes());
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
        @Override public Destination destination(PartitionService partitionService, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.nodes());

            return new RandomNode(m.nodes());
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
        @Override public Destination destination(PartitionService partitionService, NodesMapping m, ImmutableIntList k) {
            assert m != null && m.nodes() != null && m.nodes().size() == 1;

            return new AllNodes(Collections
                .singletonList(Objects
                    .requireNonNull(F
                        .first(m.nodes()))));
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
        @Override public Destination destination(PartitionService partitionService, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.assignments()) && !k.isEmpty();

            List<List<UUID>> assignments = m.assignments();

            if (U.assertionsEnabled()) {
                for (List<UUID> assignment : assignments)
                    assert F.isEmpty(assignment) || assignment.size() == 1;
            }

            return new Partitioned(m.nodes(), assignments, partitionFunction(partitionService, assignments.size(), k));
        }

        /** {@inheritDoc} */
        @Override public ToIntFunction<Object> partitionFunction(PartitionService partitionService, int partitionsCount, ImmutableIntList k) {
            return DistributionFunction.rowToPart(partitionService.partitionFunction(CU.UNDEFINED_CACHE_ID), partitionsCount, k.toIntArray());
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

        /** */
        public int cacheId() {
            return cacheId;
        }

        /** {@inheritDoc} */
        @Override public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override public Destination destination(PartitionService partitionService, NodesMapping m, ImmutableIntList k) {
            assert m != null && !F.isEmpty(m.assignments()) && k.size() == 1;

            List<List<UUID>> assignments = m.assignments();

            if (U.assertionsEnabled()) {
                for (List<UUID> assignment : assignments)
                    assert F.isEmpty(assignment) || assignment.size() == 1;
            }

            return new Partitioned(m.nodes(), assignments, partitionFunction(partitionService, assignments.size(), k));
        }

        /** {@inheritDoc} */
        @Override public ToIntFunction<Object> partitionFunction(PartitionService partitionService, int partitionsCount, ImmutableIntList k) {
            return DistributionFunction.rowToPart(partitionService.partitionFunction(cacheId), partitionsCount, k.toIntArray());
        }

        /** {@inheritDoc} */
        @Override protected String name0() {
            return "affinity[" + key + "]";
        }
    }

    /** */
    private static ToIntFunction<Object> rowToPart(ToIntFunction<Object> keyToPart, int parts, int[] keys) {
        return r -> {
            Object[] row = (Object[]) r;

            if (F.isEmpty(row))
                return 0;

            int hash = keyToPart.applyAsInt(row[keys[0]]);

            for (int i = 1; i < keys.length; i++)
                hash = 31 * hash + keyToPart.applyAsInt(row[keys[i]]);

            return U.safeAbs(hash) % parts;
        };
    }
}
