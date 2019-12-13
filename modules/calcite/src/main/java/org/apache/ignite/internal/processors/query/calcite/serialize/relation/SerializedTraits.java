/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.io.Serializable;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class SerializedTraits implements Serializable {
    private static final Byte CONVENTION = 0;

    private final List<Serializable> traits;

    public SerializedTraits(RelTraitSet traits) {
        this.traits = Commons.transform(traits, this::toSerializable);
    }

    public RelTraitSet toTraitSet(RelOptCluster cluster) {
        RelTraitSet traits = cluster.traitSet();

        for (Serializable trait : this.traits)
            traits.replace(fromSerializable(trait));

        return traits.simplify();
    }

    private Serializable toSerializable(RelTrait trait) {
        if (trait instanceof Serializable)
            return (Serializable) trait;
        if (trait == IgniteConvention.INSTANCE)
            return CONVENTION;

        throw new AssertionError();
    }

    private RelTrait fromSerializable(Serializable trait) {
        if (trait instanceof RelTrait)
            return (RelTrait) trait;
        if (CONVENTION.equals(trait))
            return IgniteConvention.INSTANCE;

        throw new AssertionError();
    }
}
