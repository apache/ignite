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

package org.apache.ignite.binary;

import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryFieldImpl;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.HashMap;

/**
 * Default implementation of fields based hash code resolver.
 */
public final class BinaryFieldListIdentity implements BinaryIdentity {
    /** Mutex for synchronization. */
    private final Object mux = new Object();

    /** Cached single accessor. */
    private volatile FieldAccessor accessor;

    /** Cached accessors used when multiple (typeId, schemaId) pairs are met. */
    private volatile HashMap<Long, FieldAccessor> accessors;

    /** Field names. */
    private String[] fieldNames;

    /**
     * Default constructor.
     */
    public BinaryFieldListIdentity() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    public BinaryFieldListIdentity(BinaryFieldListIdentity other) {
        fieldNames = other.fieldNames;
    }

    /**
     * @return Fields list to hash/compare objects based upon.
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /**
     * Set field names.
     *
     * @param fieldNames Field names.
     * @return {@code this} for chaining.
     */
    public BinaryFieldListIdentity setFieldNames(String... fieldNames) {
        this.fieldNames = fieldNames;

        return this;
    }

    /** {@inheritDoc} */
    @Override public int hashCode(BinaryObject obj) {
        assert obj != null;
        assert !(obj instanceof BinaryEnumObjectImpl);
        assert fieldNames != null;

        if (obj instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

            if (obj0.hasSchema()) {
                // Handle optimized case.
                FieldAccessor accessor = accessor(obj0);

                assert accessor != null;

                return accessor.hashCode(obj0);
            }
        }

        // Handle regular case.
        int hash = 0;

        for (String fieldName : fieldNames) {
            Object val = obj.field(fieldName);

            hash = 31 * hash + (val != null ? val.hashCode() : 0);
        }

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(BinaryObject o1, BinaryObject o2) {
        assert fieldNames != null;
        assert o1 != null;
        assert o1 instanceof BinaryObjectExImpl;

        if (o1 == o2)
            return true;

        if (o2 == null || !(o2 instanceof BinaryObjectExImpl))
            return false;

        BinaryObjectExImpl ex1 = (BinaryObjectExImpl) o1;
        BinaryObjectExImpl ex2 = (BinaryObjectExImpl) o2;

        if (ex1.hasSchema() && ex2.hasSchema()) {
            FieldAccessor accessor1 = accessor(ex1);
            FieldAccessor accessor2 = accessor(ex2);

            for (int i = 0; i < fieldNames.length; i++) {
                Object val1 = accessor1.field(ex1, i);
                Object val2 = accessor2.field(ex2, i);

                if (!F.eq(val1, val2))
                    return false;
            }
        }
        else {
            BinaryType type1 = ex1.type();
            BinaryType type2 = ex1.type();

            if (type1.typeId() == type2.typeId()) {
                for (String fieldName : fieldNames) {
                    BinaryFieldImpl field = (BinaryFieldImpl)type1.field(fieldName);

                    Object val1 = field.value(ex1);
                    Object val2 = field.value(ex2);

                    if (!F.eq(val1, val2))
                        return false;
                }
            }
            else {
                for (String fieldName : fieldNames) {
                    BinaryFieldImpl field1 = (BinaryFieldImpl)type1.field(fieldName);
                    BinaryFieldImpl field2 = (BinaryFieldImpl)type2.field(fieldName);

                    Object val1 = field1.value(ex1);
                    Object val2 = field2.value(ex2);

                    if (!F.eq(val1, val2))
                        return false;
                }
            }
        }

        return true;
    }

    /**
     * Get fields accessor for the given object.
     *
     * @param obj Object.
     * @return Accessor.
     */
    private FieldAccessor accessor(BinaryObjectExImpl obj) {
        int typeId = obj.typeId();
        int schemaId = obj.schemaId();

        // Try getting single accessor.
        FieldAccessor res = accessor;

        if (res != null && res.applicableTo(typeId, schemaId))
            return res;

        // Try reading form map.
        long key = (long)typeId << 32 + schemaId;

        HashMap<Long, FieldAccessor> accessors0 = accessors;

        if (accessors0 != null) {
            res = accessors0.get(key);

            if (res != null)
                return res;
        }

        // Failed to get from cache, go to locking.
        synchronized (mux) {
            // Create accessor.
            int[] orders = new int[fieldNames.length];

            BinaryType type = obj.type();

            for (int i = 0; i < fieldNames.length; i++) {
                BinaryFieldImpl field = (BinaryFieldImpl)type.field(fieldNames[i]);

                orders[i] = field.fieldOrder(obj);
            }

            res = new FieldAccessor(typeId, schemaId, orders);

            // Set accessor.
            if (accessors != null) {
                accessors0 = new HashMap<>(accessors);

                accessors0.put(key, res);

                accessors = accessors0;
            }
            else if (accessor == null)
                accessor = res;
            else {
                accessors0 = new HashMap<>();

                accessors0.put(key, res);

                accessors = accessors0;
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldListIdentity.class, this);
    }

    /**
     * Optimized fields accessor.
     */
    private static class FieldAccessor {
        /** Type ID. */
        private final int typeId;

        /** Schema ID. */
        private final int schemaId;

        /** Field orders. */
        private final int[] orders;

        /**
         * Constructor.
         *
         * @param typeId Type ID.
         * @param schemaId Schema ID.
         */
        private FieldAccessor(int typeId, int schemaId, int[] orders) {
            this.typeId = typeId;
            this.schemaId = schemaId;
            this.orders = orders;
        }

        /**
         * Check whether object is applicable to that hash code accessor.
         * @param expTypeId Expected schema ID.
         * @param expSchemaId Expected schema ID.
         * @return {@code True} if matches.
         */
        private boolean applicableTo(int expTypeId, int expSchemaId) {
            return typeId == expTypeId && schemaId == expSchemaId;
        }

        /**
         * Get field on the given index.
         *
         * @param obj Object.
         * @param idx Index.
         * @return Field value.
         */
        private Object field(BinaryObjectExImpl obj, int idx) {
            int order = orders[idx];

            return obj.fieldByOrder(order);
        }

        /**
         * Calculate object hash code.
         *
         * @param obj Object.
         * @return Hash code.
         */
        private int hashCode(BinaryObjectExImpl obj) {
            int hash = 0;

            for (int order : orders) {
                Object val = obj.fieldByOrder(order);

                hash = 31 * hash + (val != null ? val.hashCode() : 0);
            }

            return hash;
        }
    }
}
