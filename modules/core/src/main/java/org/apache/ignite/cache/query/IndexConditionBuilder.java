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

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.cache.query.IndexCondition;
import org.apache.ignite.internal.cache.query.RangeIndexCondition;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Factory for {@link IndexCondition} for {@link IndexQuery}.
 */
public class IndexConditionBuilder {
    /** Object to mark a boundary if {@code null} is specified. */
    private static final Object NULL = new Null();

    /** Less Then. */
    public static IndexCondition lt(String field, Object val) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexCondition(field, null, wrapNull(val));
    }

    /** Less Then or Equal. */
    public static IndexCondition lte(String field, Object val) {
        IndexCondition idxCond = lt(field, val);

        ((RangeIndexCondition) idxCond).upperInclusive(true);

        return idxCond;
    }

    /** Greater Then. */
    public static IndexCondition gt(String field, Object val) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexCondition(field, wrapNull(val), null);
    }

    /** Greater Then or Equal. */
    public static IndexCondition gte(String field, Object val) {
        IndexCondition idxCond = gt(field, val);

        ((RangeIndexCondition) idxCond).lowerInclusive(true);

        return idxCond;
    }

    /** Between. Lower and upper boundaries are inclusive. */
    public static IndexCondition between(String field, Object lower, Object upper) {
        A.notNullOrEmpty(field, "field");

        RangeIndexCondition cond = new RangeIndexCondition(field, wrapNull(lower), wrapNull(upper));

        cond.lowerInclusive(true);
        cond.upperInclusive(true);

        return cond;
    }

    /** */
    private static Object wrapNull(Object val) {
        return val == null ? NULL : val;
    }

    /** Class to represent NULL value. */
    public static final class Null {}
}
