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

import java.beans.PropertyDescriptor;
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
     * Constructs Ignite cache key POJO object descriptor.
     *
     * @param desc property descriptor.
     */
    public PojoKeyField(PropertyDescriptor desc) {
        super(desc);
    }

    /**
     * Returns sort order for the field.
     *
     * @return sort order.
     */
    public SortOrder getSortOrder() {
        return sortOrder;
    }

    /** {@inheritDoc} */
    @Override protected void init(QuerySqlField sqlField) {
        if (sqlField.descending())
            sortOrder = SortOrder.DESC;
    }
}
