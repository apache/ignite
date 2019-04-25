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

package org.apache.ignite.internal.stat;

import java.util.Objects;

/**
 * Statistics holder key
 */
public class IoStatisticsHolderKey {
    /** Name. */
    private final String name;

    /** Second name. */
    private final String subName;

    /**
     * Constructor.
     *
     * @param name Name.
     */
    public IoStatisticsHolderKey(String name) {
        this(name, null);
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param subName Second name.
     */
    public IoStatisticsHolderKey(String name, String subName) {
        assert name != null;

        this.name = name;
        this.subName = subName;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Second name.
     */
    public String subName() {
        return subName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IoStatisticsHolderKey other = (IoStatisticsHolderKey)o;

        return Objects.equals(name, other.name) && Objects.equals(subName, other.subName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (name != null ? name.hashCode() : 0) + (subName != null ? subName.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return subName == null ? name : name + "." + subName;
    }
}
