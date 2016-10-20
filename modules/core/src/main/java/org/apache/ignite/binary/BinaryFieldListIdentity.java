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

    /** Cached single calculator. */
    private volatile HashCodeCalculator calc;

    /** Cached calculators used when multiple (typeId, schemaId) pairs are met. */
    private volatile HashMap<Long, HashCodeCalculator> calcs;

    /** Field names. */
    private String[] fieldNames;

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
        assert fieldNames != null;

        if (obj instanceof BinaryEnumObjectImpl)
            // Handle special case for enums.
            return obj.hashCode();
        else {
            if (obj instanceof BinaryObjectExImpl) {
                BinaryObjectExImpl obj0 = (BinaryObjectExImpl)obj;

                if (obj0.hasSchema())
                    // Handle optimized case.
                    return hashCode0(obj0);
            }

            // Handle regular case.
            int hash = 0;

            for (String fieldName : fieldNames) {
                Object val = obj.field(fieldName);

                hash = 31 * hash + (val != null ? val.hashCode() : 0);
            }

            return hash;
        }
    }

    /**
     * Optimized routine for well-known binary object classes.
     *
     * @param obj Object.
     * @return Hash code.
     */
    private int hashCode0(BinaryObjectExImpl obj) {
        HashCodeCalculator calc = hashCalculator(obj);

        assert calc != null;

        return calc.calculate(obj);
    }

    /**
     * Get hash code calculator.
     *
     * @param obj Object.
     * @return Calculator.
     */
    private HashCodeCalculator hashCalculator(BinaryObjectExImpl obj) {
        int typeId = obj.typeId();
        int schemaId = obj.schemaId();

        // Try getting single calc.
        HashCodeCalculator res = calc;

        if (res != null && res.applicableTo(typeId, schemaId))
            return res;

        // Try reading form map.
        long key = (long)typeId << 32 + schemaId;

        HashMap<Long, HashCodeCalculator> calcs0 = calcs;

        if (calcs0 != null) {
            res = calcs0.get(key);

            if (res != null)
                return res;
        }

        // Failed to get from cache, go to locking.
        synchronized (mux) {
            // Create calc.
            int[] orders = new int[fieldNames.length];

            BinaryType type = obj.type();

            for (int i = 0; i < fieldNames.length; i++) {
                BinaryFieldImpl field = (BinaryFieldImpl)type.field(fieldNames[i]);

                orders[i] = field.fieldOrder(obj);
            }

            res = new HashCodeCalculator(typeId, schemaId, orders);

            // Set calc.
            if (calcs != null) {
                calcs0 = new HashMap<>(calcs);

                calcs0.put(key, res);

                calcs = calcs0;
            }
            else if (calc == null)
                calc = res;
            else {
                calcs0 = new HashMap<>();

                calcs0.put(key, res);

                calcs = calcs0;
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(BinaryObject o1, BinaryObject o2) {
        assert fieldNames != null;

        if (o1 == o2)
            return true;

        if (o1 == null || o2 == null)
            return false;

        if (!(o1 instanceof BinaryObjectExImpl) || !(o2 instanceof BinaryObjectExImpl))
            return false;

        BinaryObjectExImpl exObj1 = (BinaryObjectExImpl) o1;

        BinaryObjectExImpl exObj2 = (BinaryObjectExImpl) o2;

        for (String fld : fieldNames) {
            if (!F.eq(fieldValue(exObj1, fld), fieldValue(exObj2, fld)))
                return false;
        }

        return true;
    }

    /**
     * @param exObj Object to get the field value from.
     * @param fieldName Field id.
     * @return Field value.
     */
    private static Object fieldValue(BinaryObjectExImpl exObj, String fieldName) {
        return exObj.context().createField(exObj.typeId(), fieldName).value(exObj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryFieldListIdentity.class, this);
    }

    /**
     * Optimized hash code calculator.
     */
    private static class HashCodeCalculator {
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
        private HashCodeCalculator(int typeId, int schemaId, int[] orders) {
            this.typeId = typeId;
            this.schemaId = schemaId;
            this.orders = orders;
        }

        /**
         * Check whether object is applicable to that hash code calculator.
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
        private int calculate(BinaryObjectExImpl obj) {
            int hash = 0;

            for (int order : orders) {
                Object val = obj.fieldByOrder(order);

                hash = 31 * hash + (val != null ? val.hashCode() : 0);
            }

            return hash;
        }
    }
}
