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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;

/**
 * Relational operator that returns the contents of a table.
 */
public abstract class AbstractIgniteSpool extends Spool implements IgniteRel {
    /** */
    public AbstractIgniteSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        Type readType,
        RelNode input
    ) {
        super(cluster, traits, input, readType, Type.EAGER);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return pw
            .input("input", getInput())
            .item("readType", readType.name())
            .item("writeType", writeType.name());
    }
}
