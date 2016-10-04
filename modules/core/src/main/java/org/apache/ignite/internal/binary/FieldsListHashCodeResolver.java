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
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectHashCodeResolver;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Default implementation of fields based hash code resolver.
 */
public final class FieldsListHashCodeResolver implements BinaryObjectHashCodeResolver {
    /**
     * Fields based on whose values hash code should be computed.
     */
    private final List<String> fieldNames;

    /**
     * @param fieldNames Fields based on whose values hash code should be computed.
     */
    public FieldsListHashCodeResolver(List<String> fieldNames) {
        if (F.isEmpty(fieldNames))
            throw new IllegalArgumentException("Empty fields list for FieldsListHashCodeResolver");

        this.fieldNames = fieldNames;
    }

    /** {@inheritDoc} */
    @Override public int hash(BinaryObjectBuilder builder) {
        int hash = 0;

        for (String fieldName : fieldNames) {
            Object val = builder.getField(fieldName);

            hash = 31 * hash + (val != null ? val.hashCode() : 0);
        }

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(BinaryObjectExImpl o1, BinaryObjectExImpl o2) {
        if (o1 == o2)
            return true;

        if (o1 == null || o2 == null)
            return false;

        for (String fieldName : fieldNames) {
            Object v1 = o1.field(fieldName);

            Object v2 = o2.field(fieldName);

            if (!F.eq(v1, v2))
                return false;
        }

        return true;
    }
}
