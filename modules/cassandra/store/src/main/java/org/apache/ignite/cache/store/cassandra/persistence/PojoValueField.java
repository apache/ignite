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
 * Descriptor for Ignite value POJO class
 */
public class PojoValueField extends PojoField {
    /** Xml attribute specifying that Cassandra column is static. */
    private static final String STATIC_ATTR = "static";

    /** Xml attribute specifying that secondary index should be created for Cassandra column. */
    private static final String INDEX_ATTR = "index";

    /** Xml attribute specifying secondary index custom class. */
    private static final String INDEX_CLASS_ATTR = "indexClass";

    /** Xml attribute specifying secondary index options. */
    private static final String INDEX_OPTIONS_ATTR = "indexOptions";

    /** Indicates if Cassandra column should be indexed. */
    private Boolean isIndexed;

    /** Custom java class for Cassandra secondary index. */
    private String idxCls;

    /** Secondary index options. */
    private String idxOptions;

    /** Indicates if Cassandra column is static. */
    private Boolean isStatic;

    /**
     * Constructs Ignite cache value field descriptor.
     *
     * @param el field descriptor xml configuration element.
     * @param pojoCls field java class
     */
    public PojoValueField(Element el, Class pojoCls) {
        super(el, pojoCls);

        if (el.hasAttribute(STATIC_ATTR))
            isStatic = Boolean.parseBoolean(el.getAttribute(STATIC_ATTR).trim().toLowerCase());

        if (el.hasAttribute(INDEX_ATTR))
            isIndexed = Boolean.parseBoolean(el.getAttribute(INDEX_ATTR).trim().toLowerCase());

        if (el.hasAttribute(INDEX_CLASS_ATTR))
            idxCls = el.getAttribute(INDEX_CLASS_ATTR).trim();

        if (el.hasAttribute(INDEX_OPTIONS_ATTR)) {
            idxOptions = el.getAttribute(INDEX_OPTIONS_ATTR).trim();

            if (!idxOptions.toLowerCase().startsWith("with")) {
                idxOptions = idxOptions.toLowerCase().startsWith("options") ?
                    "with " + idxOptions :
                    "with options = " + idxOptions;
            }
        }
    }

    /**
     * Constructs Ignite cache value field descriptor.
     *
     * @param accessor field property accessor.
     */
    public PojoValueField(PojoFieldAccessor accessor) {
        super(accessor);

        QuerySqlField sqlField = (QuerySqlField)accessor.getAnnotation(QuerySqlField.class);

        isIndexed = sqlField != null && sqlField.index();
    }

    /** {@inheritDoc} */
    @Override public String getColumnDDL() {
        String colDDL = super.getColumnDDL();

        if (isStatic != null && isStatic)
            colDDL += " static";

        return colDDL;
    }

    /**
     * Indicates if secondary index should be created for the field.
     *
     * @return true/false if secondary index should/shouldn't be created for the field.
     */
    public boolean isIndexed() {
        return isIndexed != null && isIndexed;
    }

    /**
     * Returns DDL for the field secondary index.
     *
     * @param keyspace Cassandra keyspace where index should be created.
     * @param tbl Cassandra table for which secondary index should be created.
     *
     * @return secondary index DDL.
     */
    public String getIndexDDL(String keyspace, String tbl) {
        if (isIndexed == null || !isIndexed)
            return null;

        StringBuilder builder = new StringBuilder();

        if (idxCls != null)
            builder.append("create custom index if not exists on \"").append(keyspace).append("\".\"").append(tbl).append("\"");
        else
            builder.append("create index if not exists on \"").append(keyspace).append("\".\"").append(tbl).append("\"");

        builder.append(" (\"").append(getColumn()).append("\")");

        if (idxCls != null)
            builder.append(" using '").append(idxCls).append("'");

        if (idxOptions != null)
            builder.append(" ").append(idxOptions);

        return builder.append(";").toString();
    }
}
