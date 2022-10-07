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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/** */
public class DynamicParamTypeExtractor extends IgniteRelShuttle {
    /** */
    private final SortedMap<Integer, RexDynamicParam> acc = new TreeMap<>();
    
    /** */
    private final DynamicParamsShuttle paramsShuttle = new DynamicParamsShuttle();

    /** */
    public static ParamsMetadata go(IgniteRel root) {
        DynamicParamTypeExtractor extractor = new DynamicParamTypeExtractor();
        extractor.visit(root);
        return new ParamsMetadata(extractor.acc.values());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        paramsShuttle.apply(rel.getCondition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        paramsShuttle.apply(rel.getProjects());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteNestedLoopJoin rel) {
        paramsShuttle.apply(rel.getCondition());
        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        paramsShuttle.apply(rel.getCondition());
        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMergeJoin rel) {
        paramsShuttle.apply(rel.getCondition());
        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        paramsShuttle.apply(rel.projects());
        paramsShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        paramsShuttle.apply(rel.projects());
        paramsShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSortedIndexSpool rel) {
        paramsShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteHashIndexSpool rel) {
        paramsShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        paramsShuttle.apply(rel.getSourceExpressionList());

        return super.visit(rel);
    }

    /** */
    private final class DynamicParamsShuttle extends RexShuttle {
        /** {@inheritDoc} */
        @Override public RexNode visitDynamicParam(RexDynamicParam param) {
            acc.put(param.getIndex(), param);

            return super.visitDynamicParam(param);
        }
    }

    /** */
    private static final class ParamsMetadata implements FieldsMetadata {
        /** */
        private final Collection<RexDynamicParam> params;

        /** */
        ParamsMetadata(Collection<RexDynamicParam> params) {
            this.params = params;
        }

        /** {@inheritDoc} */
        @Override public RelDataType rowType() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public List<GridQueryFieldMetadata> queryFieldsMetadata(IgniteTypeFactory typeFactory) {
            return params.stream().map(param -> {
                RelDataType paramType = param.getType();
                Type fieldCls = typeFactory.getResultClass(paramType);

                return new CalciteQueryFieldMetadata(
                    null,
                    null,
                    param.getName(),
                    fieldCls == null ? Void.class.getName() : fieldCls.getTypeName(),
                    paramType.getPrecision(),
                    paramType.getScale(),
                    paramType.isNullable()
                );
            }).collect(ImmutableList.toImmutableList());
        }
    }
}
