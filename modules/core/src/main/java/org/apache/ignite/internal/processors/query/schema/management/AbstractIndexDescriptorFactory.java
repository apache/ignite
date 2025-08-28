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

package org.apache.ignite.internal.processors.query.schema.management;

import java.util.LinkedHashMap;
import java.util.Objects;
import org.apache.ignite.internal.cache.query.index.Order;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;

/** Abstract index descriptor factory (with helper methods). */
public abstract class AbstractIndexDescriptorFactory implements IndexDescriptorFactory {
    /** */
    protected static LinkedHashMap<String, IndexKeyDefinition> indexDescriptorToKeysDefinition(
        GridQueryIndexDescriptor idxDesc,
        GridQueryTypeDescriptor typeDesc
    ) {
        LinkedHashMap<String, IndexKeyDefinition> keyDefs = new LinkedHashMap<>(idxDesc.fields().size());

        for (String field : idxDesc.fields())
            keyDefs.put(field, keyDefinition(typeDesc, field, !idxDesc.descending(field)));

        return keyDefs;
    }

    /** */
    protected static IndexKeyDefinition keyDefinition(GridQueryTypeDescriptor typeDesc, String field, boolean ascOrder) {
        Order order = new Order(ascOrder ? SortOrder.ASC : SortOrder.DESC, null);

        GridQueryProperty prop = typeDesc.property(field);

        // Try to find property by alternative key field name.
        if (prop == null && Objects.equals(field, QueryUtils.KEY_FIELD_NAME) && !F.isEmpty(typeDesc.keyFieldName()))
            prop = typeDesc.property(typeDesc.keyFieldName());

        Class<?> fieldType = Objects.equals(field, QueryUtils.KEY_FIELD_NAME) ? typeDesc.keyClass() :
            Objects.equals(field, QueryUtils.VAL_FIELD_NAME) ? typeDesc.valueClass() : prop.type().get(0);

        int fieldPrecision = prop != null ? prop.precision() : -1;

        return new IndexKeyDefinition(IndexKeyType.forClass(fieldType).code(), order, fieldPrecision);
    }
}
