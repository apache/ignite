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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.util.Objects;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Represents an implementation of {@link IgniteFeature} used to define incompatible changes in Ignite core functionality. */
public class IgniteCoreFeature implements IgniteFeature {
    /** */
    @GridToStringInclude
    private final int id;

    /** */
    public IgniteCoreFeature(int id) {
        A.ensure(id >= 0, "Feature ID must be non-negative");

        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public int id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IgniteCoreFeature that = (IgniteCoreFeature)o;

        return id == that.id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCoreFeature.class, this);
    }
}
