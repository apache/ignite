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

import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Range bounds holder for search row.
 */
public class RangeBounds extends SearchBounds {
    /** */
    private final RexNode lowerBound;

    /** */
    private final RexNode upperBound;

    /** */
    private final boolean lowerInclude;

    /** */
    private final boolean upperInclude;

    /**
     */
    public RangeBounds(
        @Nullable RexNode lowerBound,
        @Nullable RexNode upperBound,
        boolean lowerInclude,
        boolean upperInclude
    ) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerInclude = lowerInclude;
        this.upperInclude = upperInclude;
    }

    /**
     * @return Lower search bound.
     */
    public RexNode lowerBound() {
        return lowerBound;
    }

    /**
     * @return Upper search bound.
     */
    public RexNode upperBound() {
        return upperBound;
    }

    /** */
    @Override public String toString() {
        return S.toString(RangeBounds.class, this);
    }
}
