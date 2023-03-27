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

import org.apache.ignite.cache.query.IndexQueryCriterion;

/**
 * Range index criterion that applies to BPlusTree based indexes.
 */
public final class RangeIndexQueryCriterion implements IndexQueryCriterion {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index field name. */
    private final String field;

    /** Lower bound. */
    private final Object lower;

    /** Upper bound. */
    private final Object upper;

    /** Should include lower value. */
    private boolean lowerIncl;

    /** Should include upper value. */
    private boolean upperIncl;

    /** Whether lower bound is explicitly set to {@code null}. */
    private boolean lowerNull;

    /** Whether upper bound is explicitly set to {@code null}. */
    private boolean upperNull;

    /** */
    public RangeIndexQueryCriterion(String field, Object lower, Object upper) {
        this.field = field;
        this.lower = lower;
        this.upper = upper;
    }

    /** Swap boundaries. */
    public RangeIndexQueryCriterion swap() {
        RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(field, upper, lower);

        c.lowerIncl(upperIncl);
        c.upperIncl(lowerIncl);
        c.lowerNull(upperNull);
        c.upperNull(lowerNull);

        return c;
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
    public void lowerIncl(boolean lowerIncl) {
        this.lowerIncl = lowerIncl;
    }

    /** */
    public boolean lowerIncl() {
        return lowerIncl;
    }

    /** */
    public void upperIncl(boolean upperIncl) {
        this.upperIncl = upperIncl;
    }

    /** */
    public boolean upperIncl() {
        return upperIncl;
    }

    /** */
    public void lowerNull(boolean lowerNull) {
        this.lowerNull = lowerNull;
    }

    /** */
    public boolean lowerNull() {
        return lowerNull;
    }

    /** */
    public void upperNull(boolean upperNull) {
        this.upperNull = upperNull;
    }

    /** */
    public boolean upperNull() {
        return upperNull;
    }

    /** {@inheritDoc} */
    @Override public String field() {
        return field;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return field + (lowerIncl ? "[" : "(") + lower + "; " + upper + (upperIncl ? "]" : ")");
    }
}
