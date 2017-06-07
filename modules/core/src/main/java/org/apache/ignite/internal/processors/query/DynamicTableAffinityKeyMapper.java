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

import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;

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
    private final String fldName;

    /** Type id for faster type checks. */
    private transient volatile Integer typeId;

    /** Type id for faster type checks. */
    private transient volatile BinaryField fld;

    /**
     * @param typeName Type name.
     * @param fldName Field name.
     */
    DynamicTableAffinityKeyMapper(String typeName, String fldName) {
        this.typeName = typeName;
        this.fldName = fldName;
    }

    /**
     * @return Field name.
     */
    public String fieldName() {
        return fldName;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        A.notNull(key, "key");

        if (!(key instanceof BinaryObject))
            return key;

        BinaryObject binKey = (BinaryObject)key;

        if (typeId == null) {
            if (!F.eq(binKey.type().typeName(), typeName))
                return key;

            typeId = binKey.type().typeId();

            fld = binKey.type().field(fldName);
        }

        if (binKey.type().typeId() != typeId)
            return key;

        Object res = fld.value(binKey);

        return res != null ? res : key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }
}
