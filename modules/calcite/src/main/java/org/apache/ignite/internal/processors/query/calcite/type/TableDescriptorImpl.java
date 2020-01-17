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

package org.apache.ignite.internal.processors.query.calcite.type;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class TableDescriptorImpl implements TableDescriptor {
    /** */
    private final GridQueryTypeDescriptor queryTypeDesc;

    /** */
    private final List<RelDataTypeField> extend;

    /** */
    private final Object affinityIdentity;

    /** */
    private final int affinityFieldIdx;

    /** */
    private final int cacheId;

    public TableDescriptorImpl(String cacheName, GridQueryTypeDescriptor queryTypeDesc, Object affinityIdentity) {
        cacheId = CU.cacheId(cacheName);

        this.queryTypeDesc = queryTypeDesc;
        this.affinityIdentity = affinityIdentity;

        extend = ImmutableList.of();

        String affinityField = queryTypeDesc.affinityKey();

        if (affinityField == null)
            affinityFieldIdx = -1;
        else {
            int idx = 0;

            for (String s : queryTypeDesc.fields().keySet()) {
                if (affinityField.equals(s))
                    break;

                idx++;
            }

            affinityFieldIdx = idx;
        }
    }

    private TableDescriptorImpl(int cacheId, int affinityFieldIdx, Object affinityIdentity, GridQueryTypeDescriptor queryTypeDesc, List<RelDataTypeField> extend) {
        this.queryTypeDesc = queryTypeDesc;
        this.extend = extend;
        this.affinityIdentity = affinityIdentity;

        this.cacheId = cacheId;

        if (affinityFieldIdx == -1 && !F.isEmpty(extend)) {
            for (int i = 0; i < extend.size(); i++) {
                if (QueryUtils.KEY_FIELD_NAME.equalsIgnoreCase(extend.get(i).getName())) {
                    affinityFieldIdx = queryTypeDesc.fields().size() + i;
                    break;
                }
            }
        }

        this.affinityFieldIdx = affinityFieldIdx;
    }

    @Override public RelDataType apply(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        for (Map.Entry<String, Class<?>> field : queryTypeDesc.fields().entrySet())
            b.add(field.getKey(), factory.createJavaType(field.getValue()));

        for (RelDataTypeField field : extend)
            b.add(field);

        return b.build();
    }

    @Override public IgniteDistribution distribution() {
        if (affinityIdentity == null)
            return IgniteDistributions.broadcast();

        if (affinityFieldIdx == -1)
            return IgniteDistributions.random();

        return IgniteDistributions.hash(ImmutableIntList.of(affinityFieldIdx), new DistributionFunction.AffinityDistribution(cacheId, affinityIdentity));
    }

    @Override public List<RelCollation> collations() {
        return ImmutableList.of();
    }

    @Override public int cacheId() {
        return cacheId;
    }

    @Override public Map<String, Class<?>> fields() {
        return queryTypeDesc.fields();
    }

    @Override public List<RelDataTypeField> extended() {
        return extend;
    }

    @Override public TableDescriptor extend(List<RelDataTypeField> fields) {
        List<RelDataTypeField> extend = RelOptUtil.deduplicateColumns(this.extend, fields);

        checkExtended(fields);

        return new TableDescriptorImpl(cacheId, affinityFieldIdx, affinityIdentity, queryTypeDesc, extend);
    }


    @Override public boolean matchType(CacheDataRow row) {
        return queryTypeDesc.matchType(row.value());
    }

    @Override public <T> T toRow(ExecutionContext ectx, GridCacheContext<?, ?> cctx, CacheDataRow row) throws IgniteCheckedException {
        Object[] res = new Object[queryTypeDesc.fields().size() + extend.size()];

        int i = 0;

        for (String field : queryTypeDesc.fields().keySet())
            res[i++] = cctx.unwrapBinaryIfNeeded(queryTypeDesc.value(field, row.key(), row.value()), ectx.keepBinary());

        for (RelDataTypeField field : extend) {
            if (QueryUtils.KEY_FIELD_NAME.equalsIgnoreCase(field.getName()))
                res[i++] = cctx.unwrapBinaryIfNeeded(row.key(), ectx.keepBinary());
            else if (QueryUtils.VAL_FIELD_NAME.equalsIgnoreCase(field.getName()))
                res[i++] = cctx.unwrapBinaryIfNeeded(row.value(), ectx.keepBinary());
            else
                throw new AssertionError();
        }

        return (T) res;
    }

    /** */
    private void checkExtended(List<RelDataTypeField> fields) {
        for (RelDataTypeField field : fields) {
            String name = field.getName();

            if (!QueryUtils.VAL_FIELD_NAME.equalsIgnoreCase(name)
                && !QueryUtils.KEY_FIELD_NAME.equalsIgnoreCase(name))
                throw new IgniteSQLException("Cannot extend a field. [fieldName=" + name + "]", IgniteQueryErrorCode.PARSING);
        }
    }
}
