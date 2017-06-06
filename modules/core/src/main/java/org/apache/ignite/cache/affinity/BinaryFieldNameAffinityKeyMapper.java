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

package org.apache.ignite.cache.affinity;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;

/**
 * Trivial mapper to take extract field value from binary object of specific type as affinity key.
 */
@SuppressWarnings("deprecation")
public class BinaryFieldNameAffinityKeyMapper implements AffinityKeyMapper {
    /** Type name. */
    private final String typeName;

    /** Type id for faster type checks. */
    private transient volatile Integer typeId;

    /** Field name. */
    private final String fieldName;

    /** Logger. */
    @LoggerResource
    protected transient IgniteLogger log;

    /**
     * @param typeName Type name.
     * @param fieldName Field name.
     */
    public BinaryFieldNameAffinityKeyMapper(String typeName, String fieldName) {
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
        A.notNull(key, "key");

        if (!(key instanceof BinaryObject)) {
            U.warn(log, "");

            return key;
        }

        BinaryObject binKey = (BinaryObject)key;

        if (typeId == null)
            typeId = BinaryContext.defaultMapper().typeId(typeName);

        if (binKey.type().typeId() != typeId) {
            U.warn(log, "");

            return key;
        }

        if (!binKey.hasField(fieldName)) {
            U.warn(log, "");

            return key;
        }

        return binKey.field(fieldName);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }
}
