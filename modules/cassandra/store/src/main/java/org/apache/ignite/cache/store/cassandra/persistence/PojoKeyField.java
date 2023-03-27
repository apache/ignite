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

package org.apache.ignite.cache.store.cassandra.persistence;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.w3c.dom.Element;

/**
 * Descriptor for Ignite key POJO class
 */
public class PojoKeyField extends PojoField {
    /**
     * Specifies sort order for POJO key field
     */
    public enum SortOrder {
        /** Ascending sort order. */
        ASC,
        /** Descending sort order. */
        DESC
    }

    /** Xml attribute specifying sort order. */
    private static final String SORT_ATTR = "sort";

    /** Sort order. */
    private SortOrder sortOrder;

    /**
     * Constructs Ignite cache key POJO object descriptor.
     *
     * @param el xml configuration element.
     * @param pojoCls java class of key POJO field.
     */
    public PojoKeyField(Element el, Class pojoCls) {
        super(el, pojoCls);

        if (el.hasAttribute(SORT_ATTR)) {
            try {
                sortOrder = SortOrder.valueOf(el.getAttribute(SORT_ATTR).trim().toUpperCase());
            }
            catch (IllegalArgumentException ignored) {
                throw new IllegalArgumentException("Incorrect sort order '" + el.getAttribute(SORT_ATTR) + "' specified");
            }
        }
    }

    /**
     * Constructs instance of {@code PojoKeyField} based on the other instance and java class
     * to initialize accessor.
     *
     * @param field PojoKeyField instance
     * @param pojoCls java class of the corresponding POJO
     */
    public PojoKeyField(PojoKeyField field, Class<?> pojoCls) {
        super(field, pojoCls);

        sortOrder = field.sortOrder;
    }

    /**
     * Constructs Ignite cache key POJO object descriptor.
     *
     * @param accessor property descriptor.
     */
    public PojoKeyField(PojoFieldAccessor accessor) {
        super(accessor);

        QuerySqlField sqlField = (QuerySqlField)accessor.getAnnotation(QuerySqlField.class);

        if (sqlField != null && sqlField.descending())
            sortOrder = SortOrder.DESC;
    }

    /**
     * Returns sort order for the field.
     *
     * @return sort order.
     */
    public SortOrder getSortOrder() {
        return sortOrder;
    }
}
