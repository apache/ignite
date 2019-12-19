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

package org.apache.ignite.internal.processors.query.calcite.type;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Describes table columns.
 */
public class RowType {
    /** */
    private final String[] fields;

    /** */
    private final Class[] types;

    /** */
    private final BitSet keyFields;

    /** */
    private final int affinityKey;

    /**
     * @param fields Fields names.
     * @param types Fields types.
     * @param keyFields Key fields.
     * @param affinityKey Affinity key field index.
     */
    public RowType(String[] fields, Class[] types, BitSet keyFields, int affinityKey) {
        assert fields != null && types != null && fields.length == types.length;

        this.fields = fields;
        this.types = types;
        this.keyFields = keyFields;
        this.affinityKey = affinityKey;
    }

    /**
     * Creates RelDataType on the basis of row type description.
     *
     * @param factory Type factory.
     * @return RelDataType.
     */
    public RelDataType asRelDataType(RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);

        int len = fields.length;

        for (int i = 0; i < len; i++)
            builder.add(fields[i], factory.createJavaType(types[i]));

        return builder.build();
    }

    /**
     * @return Distribution keys.
     */
    public List<Integer> distributionKeys() {
        return ImmutableIntList.of(affinityKey);
    }

    /**
     * @param idx Checking field index.
     * @return {@code True} if the field is a key field.
     */
    public boolean isKeyField(int idx) {
        return keyFields.get(idx);
    }

    /**
     * @return RowType builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** */
    public static class Builder {
        /** */
        private int affinityKey;
        /** */
        private final LinkedHashSet<String> fields;
        /** */
        private final BitSet keyFields;
        /** */
        private final ArrayList<Class> types;

        /** */
        private Builder() {
            fields = new LinkedHashSet<>();
            types = new ArrayList<>();
            keyFields = new BitSet();

            fields.add("_key"); types.add(Object.class);
            fields.add("_val"); types.add(Object.class);
        }

        /**
         * Sets '_key' field type.
         *
         * @param type '_key' field type.
         * @return {@code this} for chaining.
         */
        public Builder key(Class type) {
            if (types.get(0) != Object.class && types.get(0) != type)
                throw new IllegalStateException("Key type is already set.");

            types.set(0, type);

            return this;
        }

        /**
         * Sets '_val' field type.
         *
         * @param type '_val' field type.
         * @return {@code this} for chaining.
         */
        public Builder val(Class type) {
            if (types.get(1) != Object.class && types.get(1) != type)
                throw new IllegalStateException("Value type is already set.");

            types.set(1, type);

            return this;
        }

        /**
         * Adds a new field.
         *
         * @param name Field name.
         * @param type Field type.
         * @return {@code this} for chaining.
         */
        public Builder field(String name, Class type) {
            if (!fields.add(name))
                throw new IllegalStateException("Field name must be unique.");

            types.add(type);

            return this;
        }

        /**
         * Adds a new key field.
         *
         * @param name Field name.
         * @param type Field type.
         * @return {@code this} for chaining.
         */
        public Builder keyField(String name, Class type) {
            return keyField(name, type, false);
        }

        /**
         * Adds a new key field.
         *
         * @param name Field name.
         * @param type Field type.
         * @param affinityKey {@code True} if the field is an affinity key field.
         * @return {@code this} for chaining.
         */
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

        /**
         * Builds RowType.
         *
         * @return Row type.
         */
        public RowType build() {
            return new RowType(fields.toArray(new String[0]), types.toArray(new Class[0]), keyFields, affinityKey);
        }
    }
}
