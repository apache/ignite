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
import org.apache.ignite.internal.binary.BinarySerializedFieldComparator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.HashMap;

/**
 * Identity resolver implementation which use the list of provided fields to calculate the hash code and to perform
 * equality checks.
 * <p>
 * Standard polynomial function with multiplier {@code 31} is used to calculate hash code. For example, for three
 * fields {@code [a, b, c]}it would be {@code hash = 31 * (31 * a + b) + c}. Order of fields is important.
 */
public class BinaryFieldIdentityResolver extends BinaryAbstractIdentityResolver {
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
    public BinaryFieldIdentityResolver() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    public BinaryFieldIdentityResolver(BinaryFieldIdentityResolver other) {
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
    public BinaryFieldIdentityResolver setFieldNames(String... fieldNames) {
        this.fieldNames = fieldNames;

        return this;
    }

    /** {@inheritDoc} */
    @Override public int hashCode0(BinaryObject obj) {
        if (obj instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

            if (obj0.hasSchema()) {
                // Handle optimized case.
                FieldAccessor accessor = accessor(obj0, obj0.typeId(), obj0.schemaId());

                assert accessor != null;

                return accessor.hashCode(obj0);
            }
        }
        else if (obj instanceof BinaryEnumObjectImpl)
            throw new BinaryObjectException("Field identity resolver cannot be used with enums: " + obj);

        // Handle regular case.
        int hash = 0;

        for (String fieldName : fieldNames) {
            Object val = obj.field(fieldName);

            hash = 31 * hash + (val != null ? val.hashCode() : 0);
        }

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals0(BinaryObject o1, BinaryObject o2) {
        if (o1 instanceof BinaryObjectExImpl && o2 instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl ex1 = (BinaryObjectExImpl) o1;
            BinaryObjectExImpl ex2 = (BinaryObjectExImpl) o2;

            int typeId = ex1.typeId();

            if (typeId != ex2.typeId())
                return false;

            if (ex1.hasSchema() && ex2.hasSchema()) {
                // Optimistic case: both objects have schemas.
                int schemaId1 = ex1.schemaId();
                int schemaId2 = ex2.schemaId();

                FieldAccessor accessor1 = accessor(ex1, typeId, schemaId1);

                FieldAccessor accessor2;

                if (schemaId1 == schemaId2)
                    accessor2 = accessor1;
                else
                    accessor2 = accessor(ex2, typeId, schemaId2);

                // Even better case: compare fields without deserialization.
                BinarySerializedFieldComparator comp1 = ex1.createFieldComparator();
                BinarySerializedFieldComparator comp2 = ex2.createFieldComparator();

                for (int i = 0; i < fieldNames.length; i++) {
                    comp1.findField(accessor1.orders[i]);
                    comp2.findField(accessor2.orders[i]);

                    if (!BinarySerializedFieldComparator.equals(comp1, comp2))
                        return false;
                }

                return true;
            }
            else
                // Pessimistic case: object of unknown types, or without schemas. Have to read fields in usual way.
                return equalsSlow(ex1, ex2);
        }

        if (o1 instanceof BinaryEnumObjectImpl)
            throw new BinaryObjectException("Field identity resolver cannot be used with enums: " + o1);

        if (o2 instanceof BinaryEnumObjectImpl)
            throw new BinaryObjectException("Field identity resolver cannot be used with enums: " + o2);

        return o1.type().typeId() == o2.type().typeId() && equalsSlow(o1, o2);
    }

    /**
     * Slow-path equals routine: regular fields comparison.
     *
     * @param o1 Object 1.
     * @param o2 Object 2.
     * @return Result.
     */
    private boolean equalsSlow(BinaryObject o1, BinaryObject o2) {
        for (String fieldName : fieldNames) {
            Object val1 = o1.field(fieldName);
            Object val2 = o2.field(fieldName);

            if (!F.eq(val1, val2))
                return false;
        }

        return true;
    }

    /**
     * Get fields accessor for the given object.
     *
     * @param obj Object.
     * @param typId Type ID.
     * @param schemaId Schema ID.
     * @return Accessor.
     */
    private FieldAccessor accessor(BinaryObjectExImpl obj, int typId, int schemaId) {
        // Try getting single accessor.
        FieldAccessor res = accessor;

        if (res != null && res.applicableTo(typId, schemaId))
            return res;

        // Try reading from map.
        long key = ((long)typId << 32) + schemaId;

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

            res = new FieldAccessor(typId, schemaId, orders);

            // Set accessor.
            if (accessor == null)
                accessor = res;
            else {
                if (accessors == null) {
                    accessor = null;

                    accessors0 = new HashMap<>();
                }
                else
                    accessors0 = new HashMap<>(accessors);

                accessors0.put(key, res);

                accessors = accessors0;
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldIdentityResolver.class, this);
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
         * @param orders Field orders.
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
