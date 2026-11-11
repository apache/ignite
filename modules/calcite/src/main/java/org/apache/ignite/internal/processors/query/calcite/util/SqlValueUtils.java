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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** Content equality and hashing for SQL keys. */
public final class SqlValueUtils {
    /** */
    private SqlValueUtils() {
        // No-op.
    }

    /** Compares ROWs, ARRAYs and MAPs recursively, using scalar equality at the leaves. */
    // TODO https://issues.apache.org/jira/browse/IGNITE-29207
    // Replace calls to this method with Functions.deepEquals when upgrading to Calcite 1.43.0.
    public static boolean deepEquals(@Nullable Object left, @Nullable Object right) {
        if (left == right)
            return true;

        if (left == null || right == null)
            return false;

        List<?> leftList = asList(left);
        List<?> rightList = asList(right);

        if (leftList != null && rightList != null) {
            if (leftList.size() != rightList.size())
                return false;

            for (int i = 0; i < leftList.size(); i++) {
                if (!deepEquals(leftList.get(i), rightList.get(i)))
                    return false;
            }

            return true;
        }

        if (leftList != null || rightList != null)
            return false;

        if (left instanceof Map && right instanceof Map)
            return mapsEqual((Map<?, ?>)left, (Map<?, ?>)right);

        if (left.getClass().isArray() && right.getClass().isArray())
            return Arrays.deepEquals(new Object[] {left}, new Object[] {right});

        return left.equals(right);
    }

    /** Maps are unordered; each entry must match exactly one entry, including when keys contain ROWs. */
    private static boolean mapsEqual(Map<?, ?> left, Map<?, ?> right) {
        if (left.size() != right.size())
            return false;

        List<Map.Entry<?, ?>> unmatched = new ArrayList<>(right.entrySet());

        for (Map.Entry<?, ?> entry : left.entrySet()) {
            boolean found = false;

            for (int i = 0; i < unmatched.size(); i++) {
                Map.Entry<?, ?> other = unmatched.get(i);

                if (deepEquals(entry.getKey(), other.getKey()) && deepEquals(entry.getValue(), other.getValue())) {
                    unmatched.remove(i);
                    found = true;

                    break;
                }
            }

            if (!found)
                return false;
        }

        return true;
    }

    /** Hash consistent with {@link #deepEquals}. */
    // TODO https://issues.apache.org/jira/browse/IGNITE-29207
    // Replace calls with Functions.deepHashCode when upgrading to Calcite 1.43.0.
    public static int deepHashCode(@Nullable Object val) {
        if (val == null)
            return 0x789d;

        List<?> list = asList(val);

        if (list != null) {
            int hash = 1;

            for (Object item : list)
                hash = 31 * hash + deepHashCode(item);

            return hash;
        }

        if (val instanceof Map) {
            int hash = 0;

            for (Map.Entry<?, ?> entry : ((Map<?, ?>)val).entrySet())
                hash += deepHashCode(entry.getKey()) ^ deepHashCode(entry.getValue());

            return hash;
        }

        if (val.getClass().isArray())
            return Arrays.deepHashCode(new Object[] {val});

        return val.hashCode();
    }

    /**
     * Views the internal ROW ({@code Object[]}) or ARRAY ({@link List}) representation as a sequence.
     * Other {@link Iterable} implementations and {@link Iterator} are not supported.
     *
     * @param val SQL value to inspect.
     * @return List view for ROW or ARRAY values, or {@code null} for other values, including scalars,
     *     maps and primitive arrays.
     */
    private static @Nullable List<?> asList(Object val) {
        if (val instanceof List)
            return (List<?>)val;

        assert !(val instanceof Iterable) && !(val instanceof Iterator)
            : "Unsupported SQL sequence representation: " + val.getClass().getName();

        return val instanceof Object[] ? Arrays.asList((Object[])val) : null;
    }
}
