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
import org.apache.ignite.cache.query.IndexQueryCriteria;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Range index criteria that applies to BPlusTree based indexes.
 */
public class RangeIndexQueryCriteria implements IndexQueryCriteria {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of criteria fields. */
    private final List<String> fields = new ArrayList<>();

    /** List of criteria. Order is consistent with {@link #fields}. */
    private final List<RangeCriterion> fldCriteria = new ArrayList<>();

    /** */
    public RangeIndexQueryCriteria(String field, @Nullable Object lower, @Nullable Object upper, boolean lowIncl, boolean upIncl) {
        fields.add(field);

        fldCriteria.add(new RangeCriterion(lower, upper, lowIncl, upIncl));
    }

    /** {@inheritDoc} */
    @Override public List<String> fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public IndexQueryCriteria and(IndexQueryCriteria criteria) {
        A.ensure(criteria instanceof RangeIndexQueryCriteria, "Expect a range criteria for chaining.");

        RangeIndexQueryCriteria rngCrit = (RangeIndexQueryCriteria) criteria;

        for (int i = 0; i < rngCrit.fields.size(); i++) {
            String f = rngCrit.fields.get(i);

            A.ensure(!fields.contains(f), "Duplicated field in criteria: " + f + ".");

            fields.add(f);
            fldCriteria.add(rngCrit.fldCriteria.get(i));
        }

        return this;
    }

    /** */
    public List<RangeCriterion> criteria() {
        return fldCriteria;
    }

    /** Represents signle field criterion. */
    public static class RangeCriterion implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Lower bound. */
        private final Object lower;

        /** Upper bound. */
        private final Object upper;

        /** Should include lower value. */
        private final boolean lowerIncl;

        /** Should include upper value. */
        private final boolean upperIncl;

        /** */
        RangeCriterion(Object lower, Object upper, boolean lowerIncl, boolean upperIncl) {
            this.lower = lower;
            this.upper = upper;
            this.lowerIncl = lowerIncl;
            this.upperIncl = upperIncl;
        }

        /** Swap boundaries. */
        public RangeCriterion swap() {
            return new RangeCriterion(upper, lower, upperIncl, lowerIncl);
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
