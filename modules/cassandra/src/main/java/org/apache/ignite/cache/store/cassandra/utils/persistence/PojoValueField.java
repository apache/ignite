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

package org.apache.ignite.cache.store.cassandra.utils.persistence;

import java.beans.PropertyDescriptor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.w3c.dom.Element;

/**
 * Descriptor for Ignite value POJO class
 */
public class PojoValueField extends PojoField {
    /** */
    private static final String STATIC_ATTR = "static";

    /** */
    private static final String INDEX_ATTR = "index";

    /** */
    private static final String INDEX_CLASS_ATTR = "indexClass";

    /** */
    private static final String INDEX_OPTIONS_ATTR = "indexOptions";

    /** TODO IGNITE-1371: add comment */
    private Boolean isIndexed;

    /** TODO IGNITE-1371: add comment */
    private String idxCls;

    /** TODO IGNITE-1371: add comment */
    private String idxOptions;

    /** TODO IGNITE-1371: add comment */
    private Boolean isStatic;

    /** TODO IGNITE-1371: add comment */
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

    /** TODO IGNITE-1371: add comment */
    public PojoValueField(PropertyDescriptor desc) {
        super(desc);
    }

    /** TODO IGNITE-1371: add comment */
    public String getColumnDDL() {
        String colDDL = super.getColumnDDL();

        if (isStatic != null && isStatic)
            colDDL = colDDL + " static";

        return colDDL;
    }

    /** TODO IGNITE-1371: add comment */
    public boolean isIndexed() {
        return isIndexed != null && isIndexed;
    }

    /** TODO IGNITE-1371: add comment */
    public String getIndexDDL(String keyspace, String tbl) {
        if (isIndexed == null || !isIndexed)
            return null;

        StringBuilder builder = new StringBuilder();

        if (idxCls != null)
            builder.append("create custom index if not exists on ").append(keyspace).append(".").append(tbl);
        else
            builder.append("create index if not exists on ").append(keyspace).append(".").append(tbl);

        builder.append(" (").append(getColumn()).append(")");

        if (idxCls != null)
            builder.append(" using '").append(idxCls).append("'");

        if (idxOptions != null)
            builder.append(" ").append(idxOptions);

        return builder.append(";").toString();
    }

    /** TODO IGNITE-1371: add comment */
    protected void init(QuerySqlField sqlField) {
        if (sqlField.index())
            isIndexed = true;
    }
}
