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

import static org.apache.ignite.internal.processors.query.calcite.Stubs.intFoo;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Distribution function.
 */
public abstract class DistributionFunction {
    /**
     *
     */
    private String name;

    /**
     *
     */
    private DistributionFunction() {
        // No-op.
    }

    /**
     * @return Distribution function type.
     */
    public abstract RelDistribution.Type type();

    /**
     * @return Function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    public final String name() {
        if (name != null) {
            return name;
        }

        return name = name0().intern();
    }

    /**
     *
     */
    public boolean affinity() {
        return false;
    }

    /**
     *
     */
    public static DistributionFunction affinity(int cacheId, Object identity) {
        return new AffinityDistribution(cacheId, identity);
    }

    /**
     * Creates a destination based on this function algorithm, given nodes mapping and given distribution keys.
     *
     * @param ctx             Execution context.
     * @param affinityService Affinity function source.
     * @param group           Target mapping.
     * @param keys            Distribution keys.
     * @return Destination function.
     */
    abstract <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affinityService,
            ColocationGroup group, ImmutableIntList keys);

    /**
     * @return Function name. This name used for equality checking and in {@link RelNode#getDigest()}.
     */
    protected String name0() {
        return type().shortName;
    }

    /** {@inheritDoc} */
    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }

    /** {@inheritDoc} */
    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof DistributionFunction) { //noinspection StringEquality
            return name() == ((DistributionFunction) obj).name();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public final String toString() {
        return name();
    }

    /**
     *
     */
    public static DistributionFunction any() {
        return AnyDistribution.INSTANCE;
    }

    /**
     *
     */
    public static DistributionFunction broadcast() {
        return BroadcastDistribution.INSTANCE;
    }

    /**
     *
     */
    public static DistributionFunction singleton() {
        return SingletonDistribution.INSTANCE;
    }

    /**
     *
     */
    public static DistributionFunction random() {
        return RandomDistribution.INSTANCE;
    }

    /**
     *
     */
    public static DistributionFunction hash() {
        return HashDistribution.INSTANCE;
    }

    /**
     *
     */
    public static boolean satisfy(DistributionFunction f0, DistributionFunction f1) {
        if (f0 == f1 || f0.name() == f1.name()) {
            return true;
        }

        return f0 instanceof AffinityDistribution && f1 instanceof AffinityDistribution
                && Objects.equals(((AffinityDistribution) f0).identity(), ((AffinityDistribution) f1).identity());
    }

    /**
     *
     */
    private static final class AnyDistribution extends DistributionFunction {
        /**
         *
         */
        public static final DistributionFunction INSTANCE = new AnyDistribution();

        /**
         *
         */
        public static DistributionFunction affinity(int cacheId, Object identity) {
            return new AffinityDistribution(cacheId, identity);
        }

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.ANY;
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affinityService,
                ColocationGroup m, ImmutableIntList k) {
            throw new IllegalStateException();
        }
    }

    /**
     *
     */
    private static final class BroadcastDistribution extends DistributionFunction {
        /**
         *
         */
        public static final DistributionFunction INSTANCE = new BroadcastDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.BROADCAST_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affinityService,
                ColocationGroup m, ImmutableIntList k) {
            assert m != null && !nullOrEmpty(m.nodeIds());

            return new AllNodes<>(m.nodeIds());
        }
    }

    /**
     *
     */
    private static final class RandomDistribution extends DistributionFunction {
        /**
         *
         */
        public static final DistributionFunction INSTANCE = new RandomDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.RANDOM_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affinityService,
                ColocationGroup m, ImmutableIntList k) {
            assert m != null && !nullOrEmpty(m.nodeIds());

            return new RandomNode<>(m.nodeIds());
        }
    }

    /**
     *
     */
    private static final class SingletonDistribution extends DistributionFunction {
        /**
         *
         */
        public static final DistributionFunction INSTANCE = new SingletonDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.SINGLETON;
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affinityService,
                ColocationGroup m, ImmutableIntList k) {
            if (m == null || m.nodeIds() == null || m.nodeIds().size() != 1) {
                throw new IllegalStateException();
            }

            return new AllNodes<>(Collections.singletonList(Objects.requireNonNull(first(m.nodeIds()))));
        }
    }

    /**
     *
     */
    private static final class HashDistribution extends DistributionFunction {
        public static final DistributionFunction INSTANCE = new HashDistribution();

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affSrvc,
                ColocationGroup m, ImmutableIntList k) {
            assert m != null && !nullOrEmpty(m.assignments()) && !k.isEmpty();

            List<List<String>> assignments = m.assignments();

            if (IgniteUtils.assertionsEnabled()) {
                for (List<String> assignment : assignments) {
                    assert nullOrEmpty(assignment) || assignment.size() == 1;
                }
            }

            AffinityAdapter<RowT> affinity = new AffinityAdapter<>(affSrvc.affinity(intFoo()/*CU.UNDEFINED_CACHE_ID*/), k.toIntArray(),
                    ctx.rowHandler());

            return new Partitioned<>(assignments, affinity);
        }
    }

    /**
     *
     */
    private static final class AffinityDistribution extends DistributionFunction {
        /**
         *
         */
        private final int cacheId;

        /**
         *
         */
        private final Object identity;

        /**
         * @param cacheId  Cache ID.
         * @param identity Affinity identity key.
         */
        private AffinityDistribution(int cacheId, Object identity) {
            this.cacheId = cacheId;
            this.identity = identity;
        }

        /** {@inheritDoc} */
        @Override
        public boolean affinity() {
            return true;
        }

        /** {@inheritDoc} */
        @Override
        public RelDistribution.Type type() {
            return RelDistribution.Type.HASH_DISTRIBUTED;
        }

        /** {@inheritDoc} */
        @Override
        public <RowT> Destination<RowT> destination(ExecutionContext<RowT> ctx, AffinityService affSrvc,
                ColocationGroup m, ImmutableIntList k) {
            assert m != null && !nullOrEmpty(m.assignments()) && k.size() == 1;

            List<List<String>> assignments = m.assignments();

            if (IgniteUtils.assertionsEnabled()) {
                for (List<String> assignment : assignments) {
                    assert nullOrEmpty(assignment) || assignment.size() == 1;
                }
            }

            AffinityAdapter<RowT> affinity = new AffinityAdapter<>(affSrvc.affinity(cacheId), k.toIntArray(), ctx.rowHandler());

            return new Partitioned<>(assignments, affinity);
        }

        /**
         *
         */
        public Object identity() {
            return identity;
        }

        /** {@inheritDoc} */
        @Override
        protected String name0() {
            return "affinity[identity=" + identity + ", cacheId=" + cacheId + ']';
        }
    }
}
