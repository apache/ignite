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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 *
 */
public class TableDescriptorImpl implements TableDescriptor {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final GridQueryTypeDescriptor typeDesc;

    /** */
    private final Object affinityIdentity;

    /** */
    private final ColumnDescriptor[] descriptors;

    /** */
    private final int affinityFieldIdx;

    /** */
    public TableDescriptorImpl(GridCacheContext<?,?> cctx, GridQueryTypeDescriptor typeDesc, Object affinityIdentity) {
        this.cctx = cctx;
        this.typeDesc = typeDesc;
        this.affinityIdentity = affinityIdentity;

        String affinityField = typeDesc.affinityKey(); int affinityFieldIdx = 0;

        List<ColumnDescriptor> descriptors = new ArrayList<>();

        descriptors.add(new KeyValDescriptor(QueryUtils.KEY_FIELD_NAME, typeDesc.keyClass(), true, true));
        descriptors.add(new KeyValDescriptor(QueryUtils.VAL_FIELD_NAME, typeDesc.keyClass(), true, false));

        for (String field : this.typeDesc.fields().keySet()) {
            if (Objects.equals(affinityField, field)) {
                assert affinityFieldIdx == 0;

                affinityFieldIdx = descriptors.size();
            }

            if (Objects.equals(field, typeDesc.keyFieldAlias()))
                descriptors.add(new KeyValDescriptor(typeDesc.keyFieldAlias(), typeDesc.keyClass(), false, true));
            else if (Objects.equals(field, typeDesc.valueFieldAlias()))
                descriptors.add(new KeyValDescriptor(typeDesc.valueFieldAlias(), typeDesc.valueClass(), false, false));
            else
                descriptors.add(new FieldDescriptor(typeDesc.property(field)));
        }

        this.descriptors = descriptors.toArray(new ColumnDescriptor[0]);

        this.affinityFieldIdx = affinityFieldIdx;
    }

    /** {@inheritDoc} */
    @Override public RelDataType apply(RelDataTypeFactory factory) {
        IgniteTypeFactory f = (IgniteTypeFactory) factory;

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(f);

        for (ColumnDescriptor desc : descriptors)
            b.add(desc.name(), desc.type(f));

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<?, ?> cacheContext() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (affinityIdentity == null)
            return IgniteDistributions.broadcast();

        return IgniteDistributions.affinity(affinityFieldIdx, cctx.cacheId(), affinityIdentity);
    }

    /** {@inheritDoc} */
    @Override public List<RelCollation> collations() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public boolean match(CacheDataRow row) {
        return typeDesc.matchType(row.value());
    }

    /** {@inheritDoc} */
    @Override public <T> T toRow(ExecutionContext ectx, CacheDataRow row) throws IgniteCheckedException {
        Object[] res = new Object[descriptors.length];

        for (int i = 0; i < descriptors.length; i++)
            res[i] = descriptors[i].value(ectx, cctx, row);

        return (T) res;
    }

    /** */
    private interface ColumnDescriptor {
        /** */
        String name();

        /** */
        RelDataType type(IgniteTypeFactory f);

        /** */
        Object value(ExecutionContext ectx, GridCacheContext<?,?> cctx, CacheDataRow src) throws IgniteCheckedException;
    }

    /** */
    private static class KeyValDescriptor implements ColumnDescriptor {
        /** */
        private final String name;

        /** */
        private final Class<?> type;

        /** */
        private final boolean isSystem;

        /** */
        private final boolean isKey;

        /** */
        private KeyValDescriptor(String name, Class<?> type, boolean isSystem, boolean isKey) {
            this.name = name;
            this.type = type;
            this.isSystem = isSystem;
            this.isKey = isKey;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public RelDataType type(IgniteTypeFactory f) {
            return isSystem ? f.createSystemType(type) : f.createJavaType(type);
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext ectx, GridCacheContext<?,?> cctx, CacheDataRow src) throws IgniteCheckedException {
            return cctx.unwrapBinaryIfNeeded(isKey ? src.key() : src.value(), ectx.keepBinary());
        }
    }

    /** */
    private static class FieldDescriptor implements ColumnDescriptor {
        /** */
        private final GridQueryProperty desc;

        /** */
        private FieldDescriptor(GridQueryProperty desc) {
            this.desc = desc;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return desc.name();
        }

        /** {@inheritDoc} */
        @Override public RelDataType type(IgniteTypeFactory f) {
            return f.createJavaType(desc.type());
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext ectx, GridCacheContext<?,?> cctx, CacheDataRow src) throws IgniteCheckedException {
            return cctx.unwrapBinaryIfNeeded(desc.value(src.key(), src.value()), ectx.keepBinary());
        }
    }
}
