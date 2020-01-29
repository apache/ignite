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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;

/**
 * A superclass of all relational nodes representations.
 */
public abstract class RelGraphNode implements Serializable {
    /** */
    private final List<Serializable> traits;

    /**
     * @param traits Traits of this relational expression.
     */
    protected RelGraphNode(RelTraitSet traits) {
        assert traits.contains(IgniteConvention.INSTANCE);

        List<Serializable> list = new ArrayList<>(traits.size() - 1);

        for (RelTrait trait : traits) {
            if (trait != IgniteConvention.INSTANCE)
                list.add(toSerializable(trait));
        }

        this.traits = list;
    }

    /**
     * Perform back conversion of serializable traits representation to trait set.
     *
     * @param cluster Cluster.
     * @return Trait set.
     */
    protected RelTraitSet traitSet(RelOptCluster cluster) {
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE);

        for (Serializable trait : this.traits)
            traits = traits.replace(fromSerializable(trait));

        return traits.simplify();
    }

    /** Converts a trait to its serializable representation. */
    private Serializable toSerializable(RelTrait trait) {
        if (trait instanceof Serializable)
            return (Serializable) trait;

        throw new AssertionError();
    }

    /** Converts a serializable representation of a trait to a trait itself. */
    private RelTrait fromSerializable(Serializable trait) {
        if (trait instanceof RelTrait)
            return (RelTrait) trait;

        throw new AssertionError();
    }

    /**
     * Converts representation to particular IgniteRel.
     *
     * @param ctx Conversion context.
     * @param children Input rels.
     * @return RelNode.
     */
    public abstract IgniteRel toRel(ConversionContext ctx, List<IgniteRel> children);
}
