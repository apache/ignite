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
    private List<String> fields = new ArrayList<>();

    /** List of lower bound conditions. */
    private List<Object> lowers;

    /** List of upper bound conditions. */
    private List<Object> uppers;

    /** Whether query result includes lower bound. */
    private boolean lowerInclusive;

    /** Whether query result includes upper bound. */
    private boolean upperInclusive;

    /** Adds a condition for new index field. */
    public void addCondition(String field, @Nullable Object lower, @Nullable Object upper) {
        validate(field, lower, upper);

        fields.add(field);

        if (lower != null) {
            if (lowers == null)
                lowers = new ArrayList<>();

            lowers.add(lower);
        }

        if (upper != null) {
            if (uppers == null)
                uppers = new ArrayList<>();

            uppers.add(upper);
        }
    }

    /** */
    public List<Object> lowers() {
        return lowers;
    }

    /** */
    public List<Object> uppers() {
        return uppers;
    }

    /** */
    public void lowerInclusive(boolean val) {
        lowerInclusive = val;
    }

    /** */
    public boolean lowerInclusive() {
        return lowerInclusive;
    }

    /** */
    public void upperInclusive(boolean val) {
        upperInclusive = val;
    }

    /** */
    public boolean upperInclusive() {
        return upperInclusive;
    }

    /** Validates that new condition matches conditions on other fields. */
    private void validate(String field, @Nullable Object lower, @Nullable Object upper) {
        A.notNullOrEmpty(field, "field");

        A.ensure(!fields.contains(field),
            "Range index query already has condition for field '" + field + "'. Use 'between' instead.");

        A.ensure(!(lower != null && uppers != null),
            "Range index query supports only single boundary for different fields." +
                " For same field use 'between' instead.");

        A.ensure(!(upper != null && lowers != null),
            "Range index query supports only single boundary for different fields." +
                " For same field use 'between' instead.");
    }

    /** {@inheritDoc} */
    @Override public List<String> fields() {
        return fields;
    }
}
