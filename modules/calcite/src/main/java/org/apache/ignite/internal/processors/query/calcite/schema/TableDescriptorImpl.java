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
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class TableDescriptorImpl implements TableDescriptor {
    /** */
    private final GridQueryTypeDescriptor typeDesc;

    /** */
    private final Object affinityIdentity;

    /** */
    private final int affinityFieldIdx;

    /** */
    private final int cacheId;

    /** */
    public TableDescriptorImpl(String cacheName, GridQueryTypeDescriptor typeDesc, Object affinityIdentity) {
        cacheId = CU.cacheId(cacheName);

        this.typeDesc = typeDesc;
        this.affinityIdentity = affinityIdentity;

        affinityFieldIdx = lookupAffinityIndex(typeDesc);
    }

    /** {@inheritDoc} */
    @Override public RelDataType apply(RelDataTypeFactory factory) {
        IgniteTypeFactory f = (IgniteTypeFactory) factory;

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(f);

        b.add(QueryUtils.KEY_FIELD_NAME, f.createSystemType(typeDesc.keyClass()));
        b.add(QueryUtils.VAL_FIELD_NAME, f.createSystemType(typeDesc.valueClass()));

        for (Map.Entry<String, Class<?>> field : typeDesc.fields().entrySet())
            b.add(field.getKey(), f.createJavaType(field.getValue()));

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (affinityIdentity == null)
            return IgniteDistributions.broadcast();

        return IgniteDistributions.hash(
            ImmutableIntList.of(affinityFieldIdx),
            new DistributionFunction.AffinityDistribution(cacheId, affinityIdentity));
    }

    /** {@inheritDoc} */
    @Override public List<RelCollation> collations() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean matchType(CacheDataRow row) {
        return typeDesc.matchType(row.value());
    }

    /** {@inheritDoc} */
    @Override public <T> T toRow(ExecutionContext ectx, GridCacheContext<?, ?> cctx, CacheDataRow row) throws IgniteCheckedException {
        Object[] res = new Object[typeDesc.fields().size() + 2];

        int i = 0;

        res[i++] = cctx.unwrapBinaryIfNeeded(row.key(), ectx.keepBinary());
        res[i++] = cctx.unwrapBinaryIfNeeded(row.value(), ectx.keepBinary());

        for (String field : typeDesc.fields().keySet())
            res[i++] = cctx.unwrapBinaryIfNeeded(typeDesc.value(field, row.key(), row.value()), ectx.keepBinary());

        return (T) res;
    }

    /** */
    private int lookupAffinityIndex(GridQueryTypeDescriptor queryTypeDesc) {
        if (queryTypeDesc.affinityKey() != null) {
            int idx = 2;

            String affField = queryTypeDesc.affinityKey();

            for (String s : queryTypeDesc.fields().keySet()) {
                if (affField.equals(s))
                    return idx;

                idx++;
            }
        }

        return 0;
    }
}
