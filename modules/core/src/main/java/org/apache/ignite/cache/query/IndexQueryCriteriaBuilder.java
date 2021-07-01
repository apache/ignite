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

import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Factory of {@link IndexQueryCriterion} for {@link IndexQuery}.
 */
public class IndexQueryCriteriaBuilder {
    /** Object to mark a boundary if {@code null} is explicitly specified. */
    private static final Object NULL = new Null();

    /** Equal To. */
    public static IndexQueryCriterion eq(String field, Object val) {
        return between(field, val, val);
    }

    /** Less Then. */
    public static IndexQueryCriterion lt(String field, Object val) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexQueryCriterion(field, null, wrapNull(val), true, false);
    }

    /** Less Then or Equal. */
    public static IndexQueryCriterion lte(String field, Object val) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexQueryCriterion(field, null, wrapNull(val), true, true);
    }

    /** Greater Then. */
    public static IndexQueryCriterion gt(String field, Object val) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexQueryCriterion(field, wrapNull(val), null, false, true);
    }

    /** Greater Then or Equal. */
    public static IndexQueryCriterion gte(String field, Object val) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexQueryCriterion(field, wrapNull(val), null, true, true);
    }

    /** Between. Lower and upper boundaries are inclusive. */
    public static IndexQueryCriterion between(String field, Object lower, Object upper) {
        A.notNullOrEmpty(field, "field");

        return new RangeIndexQueryCriterion(field, wrapNull(lower), wrapNull(upper), true, true);
    }

    /** */
    private static Object wrapNull(Object val) {
        return val == null ? NULL : val;
    }

    /** Class to represent NULL value. */
    public static final class Null {}
}
