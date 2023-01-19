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

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class IgniteDistributions {
    /** */
    private static final IgniteDistribution BROADCAST = canonize(new DistributionTrait(DistributionFunction.broadcast()));

    /** */
    private static final IgniteDistribution SINGLETON = canonize(new DistributionTrait(DistributionFunction.singleton()));

    /** */
    private static final IgniteDistribution RANDOM = canonize(new DistributionTrait(DistributionFunction.random()));

    /** */
    private static final IgniteDistribution ANY = canonize(new DistributionTrait(DistributionFunction.any()));

    /**
     * @return Any distribution.
     */
    public static IgniteDistribution any() {
        return ANY;
    }

    /**
     * @return Random distribution.
     */
    public static IgniteDistribution random() {
        return RANDOM;
    }

    /**
     * @return Single distribution.
     */
    public static IgniteDistribution single() {
        return SINGLETON;
    }

    /**
     * @return Broadcast distribution.
     */
    public static IgniteDistribution broadcast() {
        return BROADCAST;
    }

    /**
     * @param key Affinity key.
     * @param cacheName Affinity cache name.
     * @param identity Affinity identity key.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(int key, String cacheName, Object identity) {
        return affinity(key, CU.cacheId(cacheName), identity);
    }

    /**
     * @param key Affinity key.
     * @param cacheId Affinity cache ID.
     * @param identity Affinity identity key.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(int key, int cacheId, Object identity) {
        return hash(ImmutableIntList.of(key), DistributionFunction.affinity(cacheId, identity));
    }

    /**
     * @param keys Affinity keys.
     * @param cacheId Affinity cache ID.
     * @param identity Affinity identity key.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(ImmutableIntList keys, int cacheId, Object identity) {
        return hash(keys, DistributionFunction.affinity(cacheId, identity));
    }

    /**
     * @param keys Distribution keys.
     * @return Hash distribution.
     */
    public static IgniteDistribution hash(List<Integer> keys) {
        return canonize(new DistributionTrait(ImmutableIntList.copyOf(keys), DistributionFunction.hash()));
    }

    /**
     * @param keys Distribution keys.
     * @param function Specific hash function.
     * @return Hash distribution.
     */
    public static IgniteDistribution hash(List<Integer> keys, DistributionFunction function) {
        return canonize(new DistributionTrait(ImmutableIntList.copyOf(keys), function));
    }

    /**
     * Creates correlated distribution, thats used to bypass set of nodes on the right hand of CNLJ in cases when
     * hash distribution cannot bypass these nodes.
     * It's an proxy for hash distribution. Nodes can't be enforced to this distribution. Original hash distribution
     * (with remapped keys) thats can be used in final plan is restored by the filter node.
     *
     * @param corrId Target distribution correlation id.
     * @param target Target distribution.
     * @return Distribution by correlate.
     */
    public static IgniteDistribution correlated(CorrelationId corrId, IgniteDistribution target) {
        return canonize(new DistributionTrait(DistributionFunction.correlated(corrId, target)));
    }

    /**
     * See {@link RelTraitDef#canonize(org.apache.calcite.plan.RelTrait)}.
     */
    private static IgniteDistribution canonize(IgniteDistribution distr) {
        return DistributionTraitDef.INSTANCE.canonize(distr);
    }
}
