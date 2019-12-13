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

package org.apache.ignite.internal.processors.query.calcite.type;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;

/**
 *
 */
public class RowType {
    private final String[] fields;
    private final Class[] types;
    private final BitSet keyFields;
    private final int affinityKey;

    public RowType(String[] fields, Class[] types, BitSet keyFields, int affinityKey) {
        this.fields = fields;
        this.types = types;
        this.keyFields = keyFields;
        this.affinityKey = affinityKey;
    }

    public RelDataType asRelDataType(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);

        int len = fields.length;

        for (int i = 0; i < len; i++)
            builder.add(fields[i], factory.createJavaType(types[i]));

        return builder.build();
    }

    public List<Integer> distributionKeys() {
        return ImmutableIntList.of(affinityKey);
    }

    public boolean isKeyField(int idx) {
        return keyFields.get(idx);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int affinityKey;
        private final LinkedHashSet<String> fields;
        private final BitSet keyFields;
        private final ArrayList<Class> types;

        private Builder() {
            fields = new LinkedHashSet<>();
            types = new ArrayList<>();
            keyFields = new BitSet();

            fields.add("_key"); types.add(Object.class);
            fields.add("_val"); types.add(Object.class);
        }

        public Builder key(Class type) {
            if (types.get(0) != Object.class && types.get(0) != type)
                throw new IllegalStateException("Key type is already set.");

            types.set(0, type);

            return this;
        }

        public Builder val(Class type) {
            if (types.get(1) != Object.class && types.get(1) != type)
                throw new IllegalStateException("Value type is already set.");

            types.set(1, type);

            return this;
        }

        public Builder field(String name, Class type) {
            if (!fields.add(name))
                throw new IllegalStateException("Field name must be unique.");

            types.add(type);

            return this;
        }

        public Builder keyField(String name, Class type) {
            if (!fields.add(name))
                throw new IllegalStateException("Field name must be unique.");

            types.add(type);

            keyFields.set(types.size() - 1);

            return this;
        }

        public Builder keyField(String name, Class type, boolean affinityKey) {
            if (affinityKey && this.affinityKey > 0)
                throw new IllegalStateException("Affinity key field must be unique.");

            if (!fields.add(name))
                throw new IllegalStateException("Field name must be unique.");

            types.add(type);

            keyFields.set(types.size() - 1);

            if (affinityKey)
                this.affinityKey = types.size() - 1;

            return this;
        }

        public RowType build() {
            return new RowType(fields.toArray(new String[0]), types.toArray(new Class[0]), keyFields, affinityKey);
        }
    }
}
