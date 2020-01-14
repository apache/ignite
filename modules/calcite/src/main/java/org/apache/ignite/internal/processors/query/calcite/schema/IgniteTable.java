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

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.processors.query.calcite.util.TableScanIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable, ScannableTable {
    /** */
    private final String schemaName;

    /** */
    private final String tableName;

    /** */
    private final String cacheName;

    /** */
    private final RowType rowType;

    /** */
    private final Object identityKey;

    /**
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param cacheName Cache name.
     * @param rowType Row type.
     * @param identityKey Affinity identity key.
     */
    public IgniteTable(String schemaName, String tableName, String cacheName, RowType rowType, Object identityKey) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.cacheName = cacheName;
        this.rowType = rowType;
        this.identityKey = identityKey;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name;
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType.asRelDataType(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return new TableStatistics();
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

        return new IgniteTableScan(cluster, traitSet, relOptTable);
    }

    /**
     * @return Table distribution trait.
     */
    public IgniteDistribution distribution() {
        Object key = identityKey();

        if (key == null)
            return IgniteDistributions.broadcast();

        return IgniteDistributions.hash(rowType.distributionKeys(), new DistributionFunction.AffinityDistribution(CU.cacheId(cacheName), key));
    }

    /**
     * @return Affinity identity key.
     */
    protected Object identityKey() {
        return identityKey;
    }

    /**
     * @param ctx Planner context.
     * @return Fragment meta information.
     */
    public FragmentInfo fragmentInfo(IgniteCalciteContext ctx) {
        return new FragmentInfo(ctx.mapForCache(CU.cacheId(cacheName)));
    }

    /** {@inheritDoc} */
    @Override public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(rows(root));
    }

    /** */
    private Iterable<Object[]> rows(DataContext root) {
        return new Helper((ExecutionContext) root, schemaName, tableName, cacheName, rowType);
    }

    /** */
    private class TableStatistics implements Statistic {
        /** {@inheritDoc} */
        @Override public Double getRowCount() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public List<RelReferentialConstraint> getReferentialConstraints() {
            return ImmutableList.of();
        }

        /** {@inheritDoc} */
        @Override public List<RelCollation> getCollations() {
            return ImmutableList.of();
        }

        /** {@inheritDoc} */
        @Override public RelDistribution getDistribution() {
            return distribution();
        }
    }

    /** */
    private static class Helper implements Iterable<Object[]> {
        /** */
        private final IgniteCalciteContext ctx;

        /** */
        private final ExecutionContext ectx;

        /** */
        private final String schemaName;

        /** */
        private final String tableName;

        /** */
        private final String cacheName;

        /** */
        private final RowType rowType;

        /** */
        private GridCacheContext<?,?> cacheContext;

        /** */
        private GridQueryTypeDescriptor desc;

        private Helper(ExecutionContext ectx, String schemaName, String tableName, String cacheName, RowType rowType) {
            ctx = ectx.parent();

            this.ectx = ectx;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.cacheName = cacheName;
            this.rowType = rowType;

            init();
        }

        private void init() {
            cacheContext = ctx.kernal().cache().context().cacheContext(CU.cacheId(cacheName));

            check(cacheContext.topology());

            desc = typeDescriptor();
        }

        private Iterator<GridDhtLocalPartition> partitions() {
            int[] parts = ectx.partitions();

            List<GridDhtLocalPartition> res = parts == null ? cacheContext.topology().localPartitions()
                : IntStream.of(parts).mapToObj(cacheContext.topology()::localPartition).collect(Collectors.toList());

            return res.iterator();
        }

        private GridQueryTypeDescriptor typeDescriptor() {
            for (GridQueryTypeDescriptor type : ctx.kernal().query().types(cacheName)) {
                if (F.eq(type.schemaName(), schemaName) && F.eq(type.tableName(), tableName))
                    return type;
            }

            throw new IgniteException("Failed to determine query type descriptor.");
        }

        private void check(GridDhtPartitionTopology topology) {
            topology.readLock();
            try {
                GridDhtTopologyFuture fut = topology.topologyVersionFuture();

                if (fut.isDone() && Objects.equals(fut.topologyVersion(), ctx.topologyVersion()))
                    return;

                throw new ClusterTopologyException("Failed to execute query. Retry on stable topology.");
            }
            finally {
                topology.readUnlock();
            }
        }

        public boolean matchType(CacheDataRow r) {
            return desc.matchType(r.value());
        }

        public Object[] toRow(CacheDataRow r) {
            String[] fields = rowType.fields();
            Object[] result = new Object[fields.length];

            for (int i = 0; i < fields.length; i++)
                result[i] = unwrapBinary(extractField(fields[i], r));

            return result;
        }

        private Object extractField(String fieldName, CacheDataRow row) {
            try {
                if (desc.keyTypeName() != null && F.eq(desc.keyTypeName(), fieldName) || QueryUtils.KEY_FIELD_NAME.equals(fieldName))
                    return row.key();
                else if (desc.valueFieldName() != null && F.eq(desc.valueFieldName(), fieldName) || QueryUtils.VAL_FIELD_NAME.equals(fieldName))
                    return row.value();
                else
                    return desc.value(fieldName, row.key(), row.value());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        private Object unwrapBinary(Object obj) {
            return cacheContext.unwrapBinaryIfNeeded(obj, false);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Object[]> iterator() {
            return new TableScanIterator<>(
                CU.cacheId(cacheName),
                partitions(),
                this::toRow,
                this::matchType);
        }
    }
}
