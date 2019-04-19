/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import java.util.Objects;

/**
 * Implementation of {@link DistributedProperty} for {@link Comparable}.
 */
public class DistributedComparableProperty<T extends Comparable<T> & Serializable> extends DistributedProperty<T> {

    /** {@inheritDoc} */
    DistributedComparableProperty(String name) {
        super(name);
    }

    /** */
    public boolean equalTo(T other) {
        return Objects.equals(val, other);
    }

    /** */
    public boolean nonEqualTo(T other) {
        return !Objects.equals(val, other);
    }

    /** */
    public boolean lessThan(T other) {
        return val.compareTo(other) < 0;
    }

    /** */
    public boolean lessOrEqualTo(T other) {
        return val.compareTo(other) <= 0;
    }

    /** */
    public boolean greaterThan(T other) {
        return val.compareTo(other) > 0;
    }

    /** */
    public boolean greaterOrEqualTo(T other) {
        return val.compareTo(other) >= 0;
    }
}
