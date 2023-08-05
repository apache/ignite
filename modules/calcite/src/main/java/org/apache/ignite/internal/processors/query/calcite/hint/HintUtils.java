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

package org.apache.ignite.internal.processors.query.calcite.hint;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.hint.SqlHintDefinition.DISABLE_RULE;
import static org.apache.ignite.internal.processors.query.calcite.hint.SqlHintDefinition.EXPAND_DISTINCT_AGG;

/** */
public class HintUtils {
    /** */
    private HintUtils() {
        // No-op.
    }

    /**
     * @return Hint if found by {@code hintDef} in {@code hints}. {@code Null} if hint is not found.
     */
    public static RelHint hint(Collection<RelHint> hints, SqlHintDefinition hintDef) {
        if (!F.isEmpty(hints)) {
            for (RelHint h : hints) {
                if (h.hintName.equals(hintDef.name()))
                    return h;
            }
        }

        return null;
    }

    /**
     * @return Hint of {@code rel} if found by {@code hintDef}. {@code Null} if hint is not found.
     */
    public static RelHint hint(RelNode rel, SqlHintDefinition hintDef) {
        return rel instanceof Hintable ? hint(((Hintable)rel).getHints(), hintDef) : null;
    }

    /**
     * @return {@code True} if {@code rel} contains hint {@code hintDef}. {@code False} otherwise,
     */
    public static boolean hasHint(RelNode rel, SqlHintDefinition hintDef) {
        return hint(rel, hintDef) != null;
    }

    /** */
    public static Collection<String> plainOptions(RelHint hint) {
        return hint == null ? Collections.emptyList() : hint.listOptions;
    }

    /** */
    public static Map<String, String> kvOptions(RelHint hint) {
        return hint == null ? Collections.emptyMap() : hint.kvOptions;
    }

    /** */
    public static boolean isExpandDistinctAggregate(LogicalAggregate rel) {
        return hasHint(rel, EXPAND_DISTINCT_AGG) && rel.getAggCallList().stream().anyMatch(AggregateCall::isDistinct);
    }
}
