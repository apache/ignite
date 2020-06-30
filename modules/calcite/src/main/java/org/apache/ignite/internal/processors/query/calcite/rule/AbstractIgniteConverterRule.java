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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;

/** */
public abstract class AbstractIgniteConverterRule<T extends RelNode> extends ConverterRule {
    /** */
    protected AbstractIgniteConverterRule(Class<T> clazz) {
        this(clazz, clazz.getName() + "Converter");
    }

    /** */
    protected AbstractIgniteConverterRule(Class<T> clazz, String descriptionPreffix) {
        super(clazz, Convention.NONE, IgniteConvention.INSTANCE, descriptionPreffix);
    }

    /** {@inheritDoc} */
    @Override public final RelNode convert(RelNode rel) {
        return convert(rel.getCluster().getPlanner(), rel.getCluster().getMetadataQuery(), (T)rel);
    }

    /**
     * Converts given rel to physical node.
     *
     * @param planner Planner.
     * @param mq Metadata query.
     * @param rel Rel node.
     * @return Physical rel.
     */
    protected abstract PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, T rel);
}
