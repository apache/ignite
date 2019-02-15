/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
