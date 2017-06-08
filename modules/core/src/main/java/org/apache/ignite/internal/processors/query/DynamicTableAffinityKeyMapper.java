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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.internal.binary.BinaryFieldEx;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Trivial mapper to take extract field value from binary object of specific type as affinity key.
 */
@SuppressWarnings("deprecation")
public class DynamicTableAffinityKeyMapper implements AffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name. */
    private final String typeName;

    /** Field name. */
    private final String fieldName;

    /** Type id for faster type checks. */
    private transient volatile BinaryFieldEx field;

    /**
     * Constructor.
     *
     * @param typeName Type name.
     * @param fieldName Field name.
     */
    DynamicTableAffinityKeyMapper(String typeName, String fieldName) {
        this.typeName = typeName;
        this.fieldName = fieldName;
    }

    /**
     * @return Field name.
     */
    public String fieldName() {
        return fieldName;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        if (!(key instanceof BinaryObject))
            return key;

        assert key instanceof BinaryObjectEx;

        BinaryObjectEx key0 = (BinaryObjectEx)key;

        if (field == null) {
            BinaryType type = key0.type();

            if (!F.eq(type.typeName(), typeName))
                return key;

            field = (BinaryFieldEx)type.field(fieldName);
        }

        if (!F.eq(key0.typeId(), field.typeId()))
            return key;

        Object affKey = field.value(key0);

        return affKey != null ? affKey : key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }
}
