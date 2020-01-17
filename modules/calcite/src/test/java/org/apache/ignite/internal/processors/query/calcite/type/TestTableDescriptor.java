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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class TestTableDescriptor implements TableDescriptor {
    /** */
    private static RelDataTypeFactory factory = new IgniteTypeFactory();

    /** */
    private List<RelDataTypeField> fields = new ArrayList<>();

    /** */
    private List<RelDataTypeField> extend = new ArrayList<>();

    /** */
    private int affinityKeyIdx;

    /** */
    private Object identityKey;

    /** */
    private String cacheName;

    /** */
    public TestTableDescriptor cacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    /** */
    public TestTableDescriptor identityKey(Object identityKey) {
        this.identityKey = identityKey;
        return this;
    }

    /** */
    public TestTableDescriptor field(String name, Class<?> type, boolean affinityKey){
        affinityKeyIdx = fields.size();
        fields.add(new RelDataTypeFieldImpl(name, fields.size(), factory.createJavaType(type)));
        return this;
    }

    /** */
    public TestTableDescriptor field(String name, Class<?> type){
        fields.add(new RelDataTypeFieldImpl(name, fields.size(), factory.createJavaType(type)));
        return this;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return CU.cacheId(cacheName);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Class<?>> fields() {
        LinkedHashMap<String, Class<?>> res = new LinkedHashMap<>();

        for (RelDataTypeField field : fields)
            res.put(field.getName(), ((RelDataTypeFactoryImpl.JavaType)field.getType()).getJavaClass());

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<RelDataTypeField> extended() {
        return extend;
    }

    /** {@inheritDoc} */
    @Override public TableDescriptor extend(List<RelDataTypeField> fields) {
        TestTableDescriptor res = new TestTableDescriptor();

        res.affinityKeyIdx = affinityKeyIdx;
        res.identityKey = identityKey;
        res.cacheName = cacheName;

        res.fields = fields;

        res.extend = RelOptUtil.deduplicateColumns(extend, fields);

        return res;
    }

    /** {@inheritDoc} */
    @Override public RelDataType apply(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        for (RelDataTypeField field : fields)
            b.add(field);

        for (RelDataTypeField field : extend)
            b.add(field);

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public List<RelCollation> collations() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (identityKey == null)
            return IgniteDistributions.broadcast();

        if (affinityKeyIdx == -1)
            return IgniteDistributions.random();

        return IgniteDistributions.hash(ImmutableIntList.of(affinityKeyIdx), new DistributionFunction.AffinityDistribution(CU.cacheId(cacheName), identityKey));

    }

    /** {@inheritDoc} */
    @Override public boolean matchType(CacheDataRow row) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public <T> T toRow(ExecutionContext ectx, GridCacheContext<?, ?> cctx, CacheDataRow row) throws IgniteCheckedException {
        throw new AssertionError();
    }
}
