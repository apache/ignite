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

package org.apache.ignite.internal.processors.query.calcite.prepare.bounds;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Multiple bounds holder for search row.
 */
public class MultiBounds extends SearchBounds {
    /** */
    @GridToStringInclude
    private final List<SearchBounds> bounds;

    /**
     */
    public MultiBounds(RexNode condition, List<SearchBounds> bounds) {
        super(condition);
        this.bounds = bounds;
    }

    /**
     * @return Search bounds.
     */
    public List<SearchBounds> bounds() {
        return bounds;
    }

    /** {@inheritDoc} */
    @Override public Type type() {
        return Type.MULTI;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return bounds.equals(((MultiBounds)o).bounds);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(bounds);
    }

    /** */
    @Override public String toString() {
        return S.toString(MultiBounds.class, this);
    }
}
