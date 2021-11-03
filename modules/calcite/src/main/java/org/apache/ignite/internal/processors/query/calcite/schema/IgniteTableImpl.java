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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements IgniteTable {
    /**
     *
     */
    private final TableDescriptor desc;
    
    /**
     *
     */
    private final Statistic statistic;
    
    /**
     *
     */
    private final Map<String, IgniteIndex> indexes = new ConcurrentHashMap<>();
    
    /**
     * @param desc Table descriptor.
     */
    public IgniteTableImpl(TableDescriptor desc) {
        this.desc = desc;
        statistic = new StatisticsImpl();
    }
    
    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }
    
    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return statistic;
    }
    
    
    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }
    
    /** {@inheritDoc} */
    @Override
    public IgniteLogicalTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        RelTraitSet traitSet = cluster.traitSetOf(distribution())
                .replace(RewindabilityTrait.REWINDABLE);
        
        return IgniteLogicalTableScan.create(cluster, traitSet, relOptTbl, null, null, null);
    }
    
    /** {@inheritDoc} */
    @Override
    public IgniteLogicalIndexScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
        RelTraitSet traitSet = cluster.traitSetOf(Convention.Impl.NONE)
                .replace(distribution())
                .replace(RewindabilityTrait.REWINDABLE)
                .replace(getIndex(idxName).collation());
        
        return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTbl, idxName, null, null, null);
    }
    
    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }
    
    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(PlanningContext ctx) {
        return desc.colocationGroup(ctx);
    }
    
    /** {@inheritDoc} */
    @Override
    public Map<String, IgniteIndex> indexes() {
        return Collections.unmodifiableMap(indexes);
    }
    
    /** {@inheritDoc} */
    @Override
    public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }
    
    /** {@inheritDoc} */
    @Override
    public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }
    
    /** {@inheritDoc} */
    @Override
    public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }
    
    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> cls) {
        if (cls.isInstance(desc)) {
            return cls.cast(desc);
        }
        
        return super.unwrap(cls);
    }
    
    /**
     *
     */
    private class StatisticsImpl implements Statistic {
        /** {@inheritDoc} */
        @Override
        public Double getRowCount() {
            return 10_000d;
        }
        
        /** {@inheritDoc} */
        @Override
        public boolean isKey(ImmutableBitSet cols) {
            return false; // TODO
        }
        
        /** {@inheritDoc} */
        @Override
        public List<ImmutableBitSet> getKeys() {
            return null; // TODO
        }
        
        /** {@inheritDoc} */
        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }
        
        /** {@inheritDoc} */
        @Override
        public List<RelCollation> getCollations() {
            return List.of(); // The method isn't used
        }
        
        /** {@inheritDoc} */
        @Override
        public IgniteDistribution getDistribution() {
            return distribution();
        }
    }
}
