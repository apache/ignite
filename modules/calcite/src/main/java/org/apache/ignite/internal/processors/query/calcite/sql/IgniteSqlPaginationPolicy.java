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
package org.apache.ignite.internal.processors.query.calcite.sql;

import java.math.RoundingMode;
import org.apache.calcite.plan.Context;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMath;
import org.jetbrains.annotations.Nullable;

/**
 * Defines a policy for processing values of SQL pagination clauses: LIMIT, FETCH, and OFFSET.
 *
 * <p>Custom instance can be supplied through {@link Frameworks.ConfigBuilder#context(Context)}.</p>
 */
@FunctionalInterface
public interface IgniteSqlPaginationPolicy {
    /** Returns the rounding mode for FETCH, LIMIT and OFFSET values. */
    RoundingMode roundingMode();

    /** Rounds the given value according to the specified policy and converts it to {@code long}. */
    static long convertToLongExact(Number value, @Nullable IgniteSqlPaginationPolicy policy) {
        RoundingMode roundingMode = policy == null ? IgniteMath.NUMERIC_ROUNDING_MODE : policy.roundingMode();
        return IgniteMath.convertToLongExact(value, roundingMode);
    }
}
