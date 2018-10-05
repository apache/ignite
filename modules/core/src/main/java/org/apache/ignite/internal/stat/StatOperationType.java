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
 *
 */

package org.apache.ignite.internal.stat;

import java.util.Objects;

/**
 * @param <V> Type of subtype of specific statistics
 */
public class StatOperationType<V> {
    /** */
    private final StatType type;
    /** */
    private final V subType;

    /**
     * @param type type of statistic.
     * @param subType sub type of statistic.
     */
    public StatOperationType(StatType type, V subType) {
        assert type != null && subType != null;

        this.type = type;
        this.subType = subType;
    }

    /**
     * @return Type.
     */
    public StatType type() {
        return type;
    }

    /**
     * @return Subtype.
     */
    public V subType() {
        return subType;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StatOperationType<?> type1 = (StatOperationType<?>)o;
        return type == type1.type &&
            Objects.equals(subType, type1.subType);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(type, subType);
    }
}
