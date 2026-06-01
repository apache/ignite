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

import java.util.Set;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptySet;

/** Field trimmer that preserves expression-based FETCH nodes. */
public class IgniteRelFieldTrimmer extends RelFieldTrimmer {
    /**  */
    IgniteRelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder) {
        super(validator, relBuilder);
    }

    /** {@inheritDoc} */
    @Override public TrimResult trimFields(
        Sort sort,
        ImmutableBitSet fieldsUsed,
        Set<RelDataTypeField> extraFields
    ) {
        if (supportedByRelBuilder(sort.fetch))
            return super.trimFields(sort, fieldsUsed, extraFields);

        RelCollation collation = sort.getCollation();
        RelNode input = sort.getInput();
        int fieldCnt = sort.getRowType().getFieldCount();

        ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();

        for (RelFieldCollation field : collation.getFieldCollations())
            inputFieldsUsed.set(field.getFieldIndex());

        TrimResult trimRes = trimChild(sort, input, inputFieldsUsed.build(), emptySet());
        RelNode newInput = trimRes.left;
        Mapping inputMapping = trimRes.right;

        if (newInput == input && inputMapping.isIdentity() && fieldsUsed.cardinality() == fieldCnt)
            return result(sort, Mappings.createIdentity(fieldCnt));

        RelNode newSort = sort.copy(
            sort.getTraitSet(),
            newInput,
            RexUtil.apply(inputMapping, collation),
            sort.offset,
            sort.fetch
        );

        return result(newSort, inputMapping, sort);
    }

    /**
     * @param node Rex node.
     * @return {@code true} if Calcite RelBuilder accepts the node for FETCH.
     */
    private static boolean supportedByRelBuilder(@Nullable RexNode node) {
        return node == null || node instanceof RexLiteral || node instanceof RexDynamicParam;
    }
}
