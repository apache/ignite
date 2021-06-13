/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cache.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Range index condition that applies to BPlusTree based indexes.
 */
public class RangeIndexCondition implements IndexCondition {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of condition fields that should match index fields. */
    private final List<String> fields = new ArrayList<>();

    /** List of condition fields that should match index fields. */
    private final List<SingleFieldRangeCondition> fldConds = new ArrayList<>();

    /** */
    public RangeIndexCondition(String field, @Nullable Object lower, @Nullable Object upper, boolean lowIncl, boolean upIncl) {
        fields.add(field);

        fldConds.add(
            new SingleFieldRangeCondition(lower, upper, lowIncl, upIncl));
    }

    /** {@inheritDoc} */
    @Override public List<String> fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public IndexCondition and(IndexCondition cond) {
        A.ensure(cond instanceof RangeIndexCondition, "Expect a range condition for chaining.");

        RangeIndexCondition rngCond = (RangeIndexCondition) cond;

        for (int i = 0; i < rngCond.fields.size(); i++) {
            String f = rngCond.fields.get(i);

            A.ensure(!fields.contains(f), "Duplicated field in conditions: " + f + ".");

            fields.add(f);
            fldConds.add(rngCond.fldConds.get(i));
        }

        return this;
    }

    /** */
    public List<SingleFieldRangeCondition> conditions() {
        return fldConds;
    }

    /** Represents info about signle field condition. */
    public static class SingleFieldRangeCondition implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** List of lower bound conditions. */
        private final Object lower;

        /** List of upper bound conditions. */
        private final Object upper;

        /** Should include lower value. */
        private final boolean lowerIncl;

        /** Should include upper value. */
        private final boolean upperIncl;

        /** */
        SingleFieldRangeCondition(Object lower, Object upper, boolean lowerIncl, boolean upperIncl) {
            this.lower = lower;
            this.upper = upper;
            this.lowerIncl = lowerIncl;
            this.upperIncl = upperIncl;
        }

        /** Swap boundaries. */
        public SingleFieldRangeCondition swap() {
            return new SingleFieldRangeCondition(upper, lower, upperIncl, lowerIncl);
        }

        /** */
        public Object lower() {
            return lower;
        }

        /** */
        public Object upper() {
            return upper;
        }

        /** */
        public boolean lowerIncl() {
            return lowerIncl;
        }

        /** */
        public boolean upperIncl() {
            return upperIncl;
        }
    }
}
