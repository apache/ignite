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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.common.TypeHandler;
import org.apache.ignite.cache.store.cassandra.common.TypeHandlerHelper;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.List;

/**
 * Descriptor for particular field in a POJO object, specifying how this field
 * should be written to or loaded from Cassandra.
 */
public abstract class PojoField implements Serializable {
    /**
     * Name attribute of XML element describing Pojo field.
     */
    private static final String NAME_ATTR = "name";

    /**
     * Column attribute of XML element describing Pojo field.
     */
    private static final String COLUMN_ATTR = "column";

    /**
     * Handler class attribute of XML element describing Pojo field.
     */
    private static final String HANDLER_CLASS_ATTR = "handlerClass";


    /**
     * Field name.
     */
    private String name;

    /**
     * Field column name in Cassandra table.
     */
    private String col;

    /**
     * Type handler for own data type.
     */
    private TypeHandler typeHandler;

    /**
     * Field column DDL.
     */
    private String colDDL;


    /**
     * Indicator for calculated field.
     */
    private Boolean calculated;

    /**
     * Field property accessor.
     */
    private transient PojoFieldAccessor accessor;

    /**
     * Checks if list contains POJO field with the specified name.
     *
     * @param fields    list of POJO fields.
     * @param fieldName field name.
     * @return true if list contains field or false otherwise.
     */
    public static boolean containsField(List<PojoField> fields, String fieldName) {
        if (fields == null || fields.isEmpty())
            return false;

        for (PojoField field : fields) {
            if (field.getName().equals(fieldName))
                return true;
        }

        return false;
    }

    /**
     * Creates instance of {@link PojoField} based on it's description in XML element.
     *
     * @param el      XML element describing Pojo field
     * @param pojoCls Pojo java class.
     */
    public PojoField(Element el, Class<?> pojoCls) {
        if (el == null)
            throw new IllegalArgumentException("DOM element representing POJO field object can't be null");

        if (!el.hasAttribute(NAME_ATTR)) {
            throw new IllegalArgumentException("DOM element representing POJO field object should have '"
                    + NAME_ATTR + "' attribute");
        }

        this.name = el.getAttribute(NAME_ATTR).trim();
        this.col = el.hasAttribute(COLUMN_ATTR) ? el.getAttribute(COLUMN_ATTR).trim() : name.toLowerCase();

        if (el.hasAttribute(HANDLER_CLASS_ATTR)) {
            typeHandler = TypeHandlerHelper.getInstanceFromClassName(el.getAttribute(HANDLER_CLASS_ATTR));
        }


        init(PropertyMappingHelper.getPojoFieldAccessor(pojoCls, name));
    }

    /**
     * Creates instance of {@link PojoField} from its field accessor.
     *
     * @param accessor field accessor.
     */
    public PojoField(PojoFieldAccessor accessor) {
        this.name = accessor.getName();

        QuerySqlField sqlField = (QuerySqlField) accessor.getAnnotation(QuerySqlField.class);

        col = sqlField != null && sqlField.name() != null && !sqlField.name().isEmpty() ?
                sqlField.name() : name.toLowerCase();

        init(accessor);
    }

    /**
     * @return field name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns java class of the field.
     *
     * @return Java class.
     */
    public Class getJavaClass() {
        return accessor.getFieldType();
    }

    /**
     * @return Cassandra table column name.
     */
    public String getColumn() {
        return col;
    }

    /**
     * @return Cassandra table column DDL statement.
     */
    public String getColumnDDL() {
        return colDDL;
    }

    /**
     * Indicates if it's a calculated field - field which value just generated based on other field values.
     * Such field will be stored in Cassandra as all other POJO fields, but it's value shouldn't be read from
     * Cassandra - cause it's again just generated based on other field values. One of the good applications of such
     * kind of fields - Cassandra materialized views build on top of other tables.
     *
     * @return {@code true} if it's auto generated field, {@code false} if not.
     */
    public boolean calculatedField() {
        if (calculated != null)
            return calculated;

        return calculated = accessor.isReadOnly();
    }

    /**
     * Gets field value as an object having Cassandra compatible type.
     * This it could be stored directly into Cassandra without any conversions.
     *
     * @param obj        Object instance.
     * @param serializer {@link org.apache.ignite.cache.store.cassandra.serializer.Serializer} to use.
     * @return Object to store in Cassandra table column.
     */
    public Object getValueFromObject(Object obj, Serializer serializer) {
        Object val = accessor.getValue(obj);

        if (val == null)
            return null;

        if (typeHandler != null) {
            return typeHandler.toCassandraPrimitiveType(val);
        }

        DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(val.getClass());

        if (cassandraType != null)
            return val;

        if (serializer == null) {
            throw new IllegalStateException("Can't serialize value from object '" +
                    val.getClass().getName() + "' field '" + name + "', cause there is no BLOB serializer specified");
        }

        return serializer.serialize(val);
    }

    /**
     * Returns POJO field annotation.
     *
     * @return annotation.
     */
    public Annotation getAnnotation(Class clazz) {
        return accessor.getAnnotation(clazz);
    }

    /**
     * Sets object field value from a {@link com.datastax.driver.core.Row} returned by Cassandra CQL statement.
     *
     * @param row        {@link com.datastax.driver.core.Row}
     * @param obj        object which field should be populated from {@link com.datastax.driver.core.Row}
     * @param serializer {@link org.apache.ignite.cache.store.cassandra.serializer.Serializer} to use.
     */
    public void setValueFromRow(Row row, Object obj, Serializer serializer) {
        if (calculatedField())
            return;

        Object val;
        if (typeHandler != null) {
            val = typeHandler.toJavaType(row, col);
        } else {
            val = PropertyMappingHelper.getCassandraColumnValue(row, col, accessor.getFieldType(), serializer);
        }

        accessor.setValue(obj, val);
    }

    /**
     * Initializes field info from property descriptor.
     *
     * @param accessor {@link PojoFieldAccessor} accessor.
     */
    private void init(PojoFieldAccessor accessor) {
        Class cassandraClazz = typeHandler != null ? typeHandler.getClazz() : accessor.getFieldType();

        DataType.Name cassandraType = PropertyMappingHelper.getCassandraType(cassandraClazz);
        cassandraType = cassandraType == null ? DataType.Name.BLOB : cassandraType;

        this.colDDL = "\"" + col + "\" " + cassandraType.toString();

        this.accessor = accessor;
    }
}
