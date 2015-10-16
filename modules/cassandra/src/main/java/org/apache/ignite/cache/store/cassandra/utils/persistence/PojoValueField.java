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
    private static final String STATIC_ATTR = "static";
    private static final String INDEX_ATTR = "index";
    private static final String INDEX_CLASS_ATTR = "indexClass";
    private static final String INDEX_OPTIONS_ATTR = "indexOptions";

    private Boolean isIndexed;
    private String indexClass;
    private String indexOptions;
    private Boolean isStatic;

    public PojoValueField(Element el, Class pojoClass) {
        super(el, pojoClass);

        if (el.hasAttribute(STATIC_ATTR))
            isStatic = Boolean.parseBoolean(el.getAttribute(STATIC_ATTR).trim().toLowerCase());

        if (el.hasAttribute(INDEX_ATTR))
            isIndexed = Boolean.parseBoolean(el.getAttribute(INDEX_ATTR).trim().toLowerCase());

        if (el.hasAttribute(INDEX_CLASS_ATTR))
            indexClass = el.getAttribute(INDEX_CLASS_ATTR).trim();

        if (el.hasAttribute(INDEX_OPTIONS_ATTR)) {
            indexOptions = el.getAttribute(INDEX_OPTIONS_ATTR).trim();

            if (!indexOptions.toLowerCase().startsWith("with")) {
                indexOptions = indexOptions.toLowerCase().startsWith("options") ?
                    "with " + indexOptions :
                    "with options = " + indexOptions;
            }
        }
    }

    public PojoValueField(PropertyDescriptor descriptor) {
        super(descriptor);
    }

    public String getColumnDDL() {
        String columnDDL = super.getColumnDDL();

        if (isStatic != null && isStatic)
            columnDDL = columnDDL + " static";

        return columnDDL;
    }

    public boolean isIndexed() {
        return isIndexed != null && isIndexed;
    }

    public String getIndexDDL(String keyspace, String table) {
        if (isIndexed == null || !isIndexed)
            return null;

        StringBuilder builder = new StringBuilder();

        if (indexClass != null)
            builder.append("create custom index if not exists on ").append(keyspace).append(".").append(table);
        else
            builder.append("create index if not exists on ").append(keyspace).append(".").append(table);

        builder.append(" (").append(getColumn()).append(")");

        if (indexClass != null)
            builder.append(" using '").append(indexClass).append("'");

        if (indexOptions != null)
            builder.append(" ").append(indexOptions);

        return builder.append(";").toString();
    }

    protected void init(QuerySqlField sqlField) {
        if (sqlField.index())
            isIndexed = true;
    }
}
