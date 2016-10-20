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

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Hash and compare instances of {@link BinaryObjectExImpl} for equality using values of all their fields.
 * This has been introduced primarily for use by DML engine to free the user from the need to worry about hash codes.
 * This hashing method should be used only in cases when you are sure that compared objects have <b>full</b> field sets.
 * Objects built via {@link IgniteBinary#toBinary} and those constructed inside DML engine satisfy that requirement.
 */
public class BinaryFullIdentity implements BinaryIdentity {
    /** {@inheritDoc} */
    @Override public int hash(BinaryObject obj) {
        A.notNull(obj, "Can't compute hash code for null object");

        assert obj instanceof BinaryObjectExImpl;

        BinaryObjectExImpl exObj = (BinaryObjectExImpl) obj;

        BinarySchema schema = exObj.createSchema();

        int hash = 0;

        for (int fieldId : schema.fieldIdsSorted()) {
            Object val = exObj.context().createField(exObj.typeId(), fieldId).value(obj);

            hash = 31 * hash + (val != null ? val.hashCode() : 0);
        }

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(BinaryObject o1, BinaryObject o2) {
        if (o1 == o2)
            return true;

        if (o1 == null || o2 == null)
            return false;

        if (!(o1 instanceof BinaryObjectExImpl) || !(o2 instanceof BinaryObjectExImpl))
            return false;

        BinaryObjectExImpl exObj1 = (BinaryObjectExImpl) o1;

        BinarySchema schema1 = exObj1.createSchema();

        BinaryObjectExImpl exObj2 = (BinaryObjectExImpl) o2;

        for (int fldId : schema1.fieldIdsSorted()) {
            if (!F.eq(fieldValue(exObj1, fldId), fieldValue(exObj2, fldId)))
                return false;
        }

        return true;
    }

    /**
     * @param exObj Object to get the field value from.
     * @param fieldId Field id.
     * @return Field value.
     */
    private static Object fieldValue(BinaryObjectExImpl exObj, int fieldId) {
        return exObj.context().createField(exObj.typeId(), fieldId).value(exObj);
    }
}
