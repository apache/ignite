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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Primitives;
import org.apache.calcite.DataContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseDataContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class PartitionExtractor extends IgniteRelShuttle {
    /** */
    private final AffinityService affSvc;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final DataContext dataContext;

    /** */
    private final Map<String, Object> params;

    /** */
    private Set<Integer> parts = null;

    /** */
    private boolean stopExtract = false;

    /** */
    private long sourceId = Long.MIN_VALUE;

    /** */
    public PartitionExtractor(AffinityService affSvc, IgniteTypeFactory typeFactory, Map<String, Object> params) {
        this.affSvc = affSvc;
        this.typeFactory = typeFactory;
        dataContext = new BaseDataContext(typeFactory);
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        if (stopExtract)
            return rel;

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
//        if (sourceId != Long.MIN_VALUE && sourceId != rel.sourceId()) {
//            stopExtract = true;
//
//            return rel;
//        }
//
//        sourceId = rel.sourceId();

        if (!tbl.descriptor().distribution().function().affinity()) {
            stopExtract = true;

            return rel;
        }

        List<SearchBounds> bounds = rel.searchBounds();

        if (F.isEmpty(bounds) && !isProperCondition(rel))
            return rel;

        int cacheId = ((CacheTableDescriptor)tbl.descriptor()).cacheInfo().cacheId();
        Set<Integer> parts0 = getConditionKeys(rel).stream().filter(Objects::nonNull)
            .map(key -> affSvc.affinity(cacheId).applyAsInt(key))
            .collect(Collectors.toSet());

        if (parts == null) {
            parts = parts0;
        }
        else {
            parts.addAll(parts0);
        }

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        if (stopExtract)
            return rel;

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        if (!tbl.descriptor().distribution().function().affinity()) {
            stopExtract = true;

            return rel;
        }

//        if (sourceId != Long.MIN_VALUE && sourceId != rel.sourceId()) {
//            stopExtract = true;
//
//            return rel;
//        }

        sourceId = rel.sourceId();

        if (!isProperCondition(rel))
            return rel;

        int cacheId = ((CacheTableDescriptor)tbl.descriptor()).cacheInfo().cacheId();
        Set<Integer> parts0 = getConditionKeys(rel).stream().filter(Objects::nonNull)
                .map(key -> affSvc.affinity(cacheId).applyAsInt(key))
                .collect(Collectors.toSet());

        if (parts == null) {
            parts = parts0;
        }
        else {
            parts.addAll(parts0);
        }

        return rel;
    }

    /** */
    public int[] go(Fragment fragment) {
        if (!(fragment.root() instanceof IgniteSender))
            return null;

        if (fragment.mapping() == null || !fragment.mapping().colocated())
            return null;

        visit(fragment.root());

        if (!F.isEmpty(parts)) {
            int[] parts0 = Ints.toArray(parts);

            Arrays.sort(parts0);

            return parts0;
        }

        return null;
    }

    /** */
    private boolean isProperCondition(ProjectableFilterableTableScan rel) {
        RexNode condition = rel.condition();

        if (condition == null)
            return false;

        if (!condition.isA(SqlKind.OR))
            return false;

        ImmutableIntList keys = ((IgniteRel)rel).distribution().getKeys();

        if (keys.size() > 1)
            return false;

        List<RexNode> operands = ((RexCall)condition).getOperands();

        return operands.stream()
            .flatMap(node -> {
                if (node.isA(SqlKind.SEARCH)) {
                    RexNode newNode = RexUtil.expandSearch(Commons.emptyCluster().getRexBuilder(), null, node);
                    if (newNode.isA(SqlKind.OR)) {
                        return ((RexCall)newNode).getOperands().stream();
                    }
                }
                return Stream.of(node);
            })
            .allMatch(cond -> {
                if (!cond.isA(SqlKind.EQUALS))
                    return false;

                List<RexNode> kv = ((RexCall)cond).getOperands();

                if (kv.size() != 2)
                    return false;

                if (!kv.get(0).isA(SqlKind.LOCAL_REF))
                    return false;

                int idx = ((RexLocalRef)kv.get(0)).getIndex();

                return idx == keys.get(0);
            });
    }

    /**
     *
     */
    private List<Object> getConditionKeys(IgniteRel rel) {
        assert rel instanceof IgniteIndexScan || rel instanceof IgniteTableScan;

        ImmutableIntList keys = rel.distribution().getKeys();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);
        RelDataType rowType = tbl.getRowType(typeFactory);

        List<Class<?>> types = new ArrayList<>(rowType.getFieldCount());
        for (RelDataType type : RelOptUtil.getFieldTypeList(rowType))
            types.add(Primitives.wrap((Class<?>)typeFactory.getJavaClass(type)));

        List<CacheColumnDescriptor> descriptors = tbl.descriptor().columnDescriptors()
                .stream().map(d -> (CacheColumnDescriptor)d).collect(Collectors.toList());

        List<CacheColumnDescriptor> keyDescriptors = keys.stream().map(idx -> descriptors.get(idx + 2))
                .collect(Collectors.toList());

        GridCacheContext<?, ?> cctx = ((CacheTableDescriptor)tbl.descriptor()).cacheContext();
        GridQueryTypeDescriptor typeDesc = ((CacheTableDescriptor)tbl.descriptor()).typeDescription();

        List<List<Object>> extrKeys;
        if (rel instanceof IgniteIndexScan && !F.isEmpty(((IgniteIndexScan)rel).searchBounds())) {
            List<SearchBounds> bounds = ((IgniteIndexScan)rel).searchBounds();

            extrKeys = keys.stream()
                .map(idx -> Pair.of(bounds.get(idx + 2), types.get(idx + 2)))
                .map(b -> extractExacts(b.left, b.right)).collect(Collectors.toList());
        }
        else {
            assert keys.size() == 1;

            RexNode condition = ((ProjectableFilterableTableScan)rel).condition();

            List<RexNode> operands = ((RexCall)condition).getOperands().stream()
                .flatMap(node -> {
                    if (node.isA(SqlKind.SEARCH)) {
                        RexNode newNode = RexUtil.expandSearch(Commons.emptyCluster().getRexBuilder(), null, node);
                        if (newNode.isA(SqlKind.OR)) {
                            return ((RexCall)newNode).getOperands().stream();
                        }
                    }
                    return Stream.of(node);
                }).collect(Collectors.toList());

            extrKeys = ImmutableList.of(operands.stream().map(cond -> Pair.of((RexCall)cond, types.get(2)))
                .map(b -> extractEquals(b.left, b.right)).collect(Collectors.toList()));
        }

        int condSz = extrKeys.stream().map(List::size).max(Integer::compareTo).orElse(0);

        if (condSz == 0)
            return ImmutableList.of();

        if (!extrKeys.stream().map(List::size).allMatch(sz -> sz == condSz))
            return ImmutableList.of();

        List<Object> res = new ArrayList<>();
        for (int i = 0; i < condSz; ++i) {
            Object key;

            if (extrKeys.size() > 1) {
                key = TypeUtils.createObject(cctx, typeDesc.keyTypeName(), typeDesc.keyClass());

                for (int j = 0; j < extrKeys.size(); ++j) {
                    CacheColumnDescriptor desc = keyDescriptors.get(j);

                    try {
                        desc.set(key, TypeUtils.fromInternal(dataContext, extrKeys.get(j).get(i), desc.storageType()));
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }
            else {
                CacheColumnDescriptor desc = keyDescriptors.get(0);

                key = TypeUtils.fromInternal(dataContext, extrKeys.get(0).get(i), desc.storageType());
            }

            if (cctx.binaryMarshaller() && key instanceof BinaryObjectBuilder)
                key = ((BinaryObjectBuilder)key).build();

            res.add(key);
        }

        return res;
    }

    /** */
    private List<Object> extractExacts(SearchBounds bounds, Class<?> colType) {
        if (bounds == null)
            return ImmutableList.of();

        if (bounds.type() == SearchBounds.Type.EXACT) {
            Object bound = extractBound((ExactBounds)bounds, colType);

            if (bound != null)
                return ImmutableList.of(bound);
        }

        if (bounds.type() == SearchBounds.Type.MULTI) {
            MultiBounds multiBounds = (MultiBounds)bounds;

            if (!multiBounds.bounds().stream().allMatch(b -> b instanceof ExactBounds))
                return ImmutableList.of();

            return multiBounds.bounds().stream().map(b -> extractBound((ExactBounds)b, colType)).collect(Collectors.toList());
        }

        return ImmutableList.of();
    }

    /** */
    private Object extractBound(ExactBounds bounds, Class<?> colType) {
        RexNode bound = bounds.bound();

        if (bound.getKind() == SqlKind.LITERAL)
            return ((RexLiteral)bound).getValueAs(colType);

        if (bound.getKind() == SqlKind.DYNAMIC_PARAM) {
            int idx = ((RexDynamicParam)bound).getIndex();

            return TypeUtils.toInternal(dataContext, params.get("?" + idx), colType);
        }

        return null;
    }

    /** */
    private Object extractEquals(RexCall cond, Class<?> colType) {
        if (!cond.isA(SqlKind.EQUALS) && cond.getOperands().size() != 2)
            return null;

        RexNode val = cond.getOperands().get(1);

        if (val.isA(SqlKind.LITERAL))
            return ((RexLiteral)val).getValueAs(colType);

        if (val.isA(SqlKind.DYNAMIC_PARAM)) {
            int idx = ((RexDynamicParam)val).getIndex();

            return TypeUtils.toInternal(dataContext, params.get("?" + idx), colType);
        }

        return null;
    }
}
