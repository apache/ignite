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

package org.apache.ignite.internal.binary;

import java.util.List;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeIdentity;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Default implementation of fields based hash code resolver.
 */
public final class FieldsListIdentity implements BinaryTypeIdentity {
    /**
     * Fields based on whose values hash code should be computed.
     */
    private List<String> fieldNames;

    /** {@inheritDoc} */
    @Override public int hash(BinaryObject obj) {
        assert obj instanceof BinaryObjectExImpl;

        BinaryObjectExImpl exObj = (BinaryObjectExImpl) obj;

        // Let's create only one reader inside builder instead of new one on each call of field()
        BinaryObjectBuilderImpl bldr = (BinaryObjectBuilderImpl) exObj.toBuilder();

        int hash = 0;

        for (String fieldName : fieldNames) {
            Object val = bldr.getField(fieldName);

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

        BinaryObjectBuilderImpl bldr1 = (BinaryObjectBuilderImpl) exObj1.toBuilder();

        BinaryObjectExImpl exObj2 = (BinaryObjectExImpl) o2;

        BinaryObjectBuilderImpl bldr2 = (BinaryObjectBuilderImpl) exObj2.toBuilder();

        for (String fld : fieldNames) {
            if (!F.eq(bldr1.getField(fld), bldr2.getField(fld)))
                return false;
        }

        return true;
    }

    /**
     * @return Fields list to hash/compare objects based upon.
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    /**
     * @param fieldNames Fields list to hash/compare objects based upon.
     */
    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }
}
