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
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Default implementation of fields based hash code resolver.
 */
public final class BinaryFieldListIdentity implements BinaryIdentity {
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
            if (obj instanceof BinaryObjectEx)
                // Handle optimized case.
                return hashCode0((BinaryObjectEx)obj);
            else {
                // Handle regular case.
                int hash = 0;

                for (String fieldName : fieldNames) {
                    Object val = obj.field(fieldName);

                    hash = 31 * hash + (val != null ? val.hashCode() : 0);
                }

                return hash;
            }
        }
    }

    /**
     * Optimized routine for well-known binary object classes.
     *
     * @param obj Object.
     * @return Hash code.
     */
    private int hashCode0(BinaryObjectEx obj) {
        // TODO.
        BinaryObjectExImpl exObj = (BinaryObjectExImpl)obj;

        int hash = 0;

        for (String fieldName : fieldNames) {
            Object val = fieldValue(exObj, fieldName);

            hash = 31 * hash + (val != null ? val.hashCode() : 0);
        }

        return hash;
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
}
