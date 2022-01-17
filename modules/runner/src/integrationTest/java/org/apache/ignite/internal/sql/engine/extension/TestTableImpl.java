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

package org.apache.ignite.internal.sql.engine.extension;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.lang.IgniteUuid;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A test table implementation.
 */
class TestTableImpl extends AbstractTable implements IgniteTable {
    private final IgniteUuid id = IgniteUuid.fromString("78b0de45-e1b4-4bc2-8261-2d0d084409de-0");

    private final TableDescriptor desc;

    /**
     * Constructor.
     *
     * @param desc A descriptor of the table.
     */
    public TestTableImpl(TableDescriptor desc) {
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override
            public @Nullable List<RelCollation> getCollations() {
                return List.of();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public TableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        RelTraitSet traits = cluster.traitSetOf(TestExtension.CONVENTION)
                .replace(desc.distribution());

        return new TestPhysTableScan(
                cluster, traits, List.of(), relOptTbl
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> cls) {
        if (cls.isInstance(desc)) {
            return cls.cast(desc);
        }

        return super.unwrap(cls);
    }
}
