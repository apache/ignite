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
package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.math.RoundingMode;
import java.util.Objects;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMath;
import org.jetbrains.annotations.Nullable;

/** Fine-grained settings that affect SQL semantics. */
public final class IgniteSqlSemantics {
    /** */
    private final RoundingMode paginationRoundingMode;

    /** */
    private IgniteSqlSemantics(Builder builder) {
        paginationRoundingMode = builder.paginationRoundingMode;
    }

    /** Returns a new builder initialized with default settings. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the rounding mode for FETCH, LIMIT and OFFSET values. */
    public RoundingMode paginationRoundingMode() {
        return paginationRoundingMode;
    }

    /** */
    public static final class Builder {
        /** */
        private RoundingMode paginationRoundingMode = IgniteMath.NUMERIC_ROUNDING_MODE;

        /** */
        private Builder() {
            // No-op.
        }

        /** Sets the rounding mode for FETCH, LIMIT and OFFSET values. */
        public Builder paginationRoundingMode(RoundingMode paginationRoundingMode) {
            this.paginationRoundingMode = Objects.requireNonNull(paginationRoundingMode);

            return this;
        }

        /** */
        public IgniteSqlSemantics build() {
            return new IgniteSqlSemantics(this);
        }
    }

    /** Rounds the given pagination value according to the specified SQL semantics and converts it to {@code long}. */
    public static long convertPaginationValueToLong(Number value, @Nullable IgniteSqlSemantics sem) {
        return sem == null ? IgniteMath.convertToLongExact(value) : IgniteMath.convertToLongExact(value, sem.paginationRoundingMode());
    }
}
