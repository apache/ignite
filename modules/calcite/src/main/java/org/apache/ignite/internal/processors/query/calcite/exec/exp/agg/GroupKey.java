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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.agg;

import java.util.Arrays;

/**
 *
 */
public class GroupKey {
    /** */
    private final Object[] fields;

    /** */
    public GroupKey(Object[] fields) {
        this.fields = fields;
    }

    /** */
    public Object field(int idx) {
        return fields[idx];
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GroupKey groupKey = (GroupKey) o;

        return Arrays.equals(fields, groupKey.fields);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(fields);
    }

    /** */
    public static Builder builder(int rowLen) {
        return new Builder(rowLen);
    }

    /** */
    public static class Builder {
        /** */
        private final Object[] fields;

        /** */
        private int idx;

        /** */
        private Builder(int rowLen) {
            fields = new Object[rowLen];
        }

        /** */
        public Builder add(Object val) {
            if (idx == fields.length)
                throw new IndexOutOfBoundsException();

            fields[idx++] = val;

            return this;
        }

        /** */
        public GroupKey build() {
            assert idx == fields.length;

            return new GroupKey(fields);
        }
    }
}
