/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.jetbrains.annotations.Nullable;

/** Utility class for rel nodes containing useful methods and constants. */
public class RelNodeUtils {
    /** Decimal of {@link Integer#MAX_VALUE} for FETCH/OFFSET bounding. */
    public static final BigDecimal DECIMAL_INT_MAX = BigDecimal.valueOf(Integer.MAX_VALUE);

    /**
     * Returns the value for FETCH with checks on that value:
     * <ul>
     *     <li>Value must not be null.</li>
     *     <li>Value must be a finite number if it's a floating point.</li>
     *     <li>Value must be non-negative.</li>
     *     <li>Value must be less than or equal to {@link Integer#MAX_VALUE}.</li>
     * </ul>
     * @param s Supplier of the value. If it is {@code null}, it will return {@code 0}.
     * @param name Name of the value.
     * @return Value for FETCH.
     * @throws IgniteSQLException If any of the checks fails.
     */
    public static int resolveFetch(@Nullable Supplier<Number> s, String name) {
        if (s == null)
            return 0;

        Number n = s.get();

        if (n == null)
            throw new IgniteSQLException(name + " must not be null");
        else if (n instanceof Double || n instanceof Float) {
            double v = n.doubleValue();

            if (!Double.isFinite(v))
                throw new IgniteSQLException(name + " must be an finite number");
        }

        BigDecimal v = new BigDecimal(n.toString()).setScale(0, RoundingMode.DOWN);

        if (v.compareTo(BigDecimal.ZERO) < 0)
            throw new IgniteSQLException(name + " must not be negative");
        else if (v.compareTo(DECIMAL_INT_MAX) > 0)
            throw new IgniteSQLException(name + " must not be greater than " + DECIMAL_INT_MAX);

        return v.intValue();
    }
}
