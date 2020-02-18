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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class ValuesConverter extends IgniteConverter {
    /** */
    public static final ConverterRule INSTANCE = new ValuesConverter();

    /**
     * Creates a ConverterRule.
     */
    protected ValuesConverter() {
        super(LogicalValues.class, "ValuesConverter");
    }

    /** {@inheritDoc} */
    @Override protected List<RelNode> convert0(@NotNull RelNode rel) {
        LogicalValues values = (LogicalValues) rel;

        RelTraitSet traits = values.getTraitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(IgniteMdDistribution.values(values.getRowType(), values.getTuples()));

        final IgniteValues newRel = new IgniteValues(values.getCluster(), values.getRowType(), values.getTuples(), traits);

        return Collections.singletonList(newRel);
    }
}
