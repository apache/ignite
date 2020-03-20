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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 * A superinterface of all Ignite relational nodes.
 */
public interface IgniteRel extends RelNode {
    /**
     * Accepts a visit from a visitor.
     *
     * @param visitor Ignite visitor.
     * @return Visit result.
     */
    <T> T accept(IgniteRelVisitor<T> visitor);

    /**
     * @return Node distribution.
     */
    default IgniteDistribution distribution() {
        return getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }

    /**
     * @return Node collations.
     */
    default List<RelCollation> collations() {
        return getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    }
}
