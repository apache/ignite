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
import java.util.LinkedList;
import java.util.List;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Stores persistence settings for Ignite cache key
 */
public class KeyPersistenceSettings extends PersistenceSettings {
    /** Partition key XML tag. */
    private static final String PARTITION_KEY_ELEMENT = "partitionKey";

    /** Cluster key XML tag. */
    private static final String CLUSTER_KEY_ELEMENT = "clusterKey";

    /** POJO field XML tag. */
    private static final String FIELD_ELEMENT = "field";

    /** POJO fields. */
    private List<PojoField> fields = new LinkedList<>();

    /** Partition key fields. */
    private List<PojoField> partKeyFields = new LinkedList<>();

    /** Cluster key fields. */
    private List<PojoField> clusterKeyFields = new LinkedList<>();

    /**
     * Creates key persistence settings object based on it's XML configuration.
     *
     * @param el XML element storing key persistence settings
     */
    public KeyPersistenceSettings(Element el) {
        super(el);

        if (PersistenceStrategy.POJO != getStrategy()) {
            init();

            return;
        }

        NodeList keyElem = el.getElementsByTagName(PARTITION_KEY_ELEMENT);

        Element partKeysNode = keyElem != null ? (Element) keyElem.item(0) : null;

        Element clusterKeysNode = el.getElementsByTagName(CLUSTER_KEY_ELEMENT) != null ?
            (Element)el.getElementsByTagName(CLUSTER_KEY_ELEMENT).item(0) : null;

        if (partKeysNode == null && clusterKeysNode != null) {
            throw new IllegalArgumentException("It's not allowed to specify cluster key fields mapping, but " +
                "doesn't specify partition key mappings");
        }

        partKeyFields = detectFields(partKeysNode, getPartitionKeyDescriptors());

        if (partKeyFields == null || partKeyFields.isEmpty()) {
            throw new IllegalStateException("Failed to initialize partition key fields for class '" +
                getJavaClass().getName() + "'");
        }

        clusterKeyFields = detectFields(clusterKeysNode, getClusterKeyDescriptors(partKeyFields));

        fields = new LinkedList<>();
        fields.addAll(partKeyFields);
        fields.addAll(clusterKeyFields);

        checkDuplicates(fields);

        init();
    }

    /** {@inheritDoc} */
    @Override public List<PojoField> getFields() {
        return fields;
    }

    /**
     * Returns Cassandra DDL for primary key.
     *
     * @return DDL statement.
     */
    public String getPrimaryKeyDDL() {
        StringBuilder partKey = new StringBuilder();

        List<String> cols = getPartitionKeyColumns();
        for (String column : cols) {
            if (partKey.length() != 0)
                partKey.append(", ");

            partKey.append("\"").append(column).append("\"");
        }

        StringBuilder clusterKey = new StringBuilder();

        cols = getClusterKeyColumns();
        if (cols != null) {
            for (String column : cols) {
                if (clusterKey.length() != 0)
                    clusterKey.append(", ");

                clusterKey.append("\"").append(column).append("\"");
            }
        }

        return clusterKey.length() == 0 ?
            "  primary key ((" + partKey + "))" :
            "  primary key ((" + partKey + "), " + clusterKey + ")";
    }

    /**
     * Returns Cassandra DDL for cluster key.
     *
     * @return Cluster key DDL.
     */
    public String getClusteringDDL() {
        StringBuilder builder = new StringBuilder();

        for (PojoField field : clusterKeyFields) {
            PojoKeyField.SortOrder sortOrder = ((PojoKeyField)field).getSortOrder();

            if (sortOrder == null)
                continue;

            if (builder.length() != 0)
                builder.append(", ");

            boolean asc = PojoKeyField.SortOrder.ASC == sortOrder;

            builder.append("\"").append(field.getColumn()).append("\" ").append(asc ? "asc" : "desc");
        }

        return builder.length() == 0 ? null : "clustering order by (" + builder + ")";
    }

    /** {@inheritDoc} */
    @Override protected String defaultColumnName() {
        return "key";
    }

    /**
     * Returns partition key columns of Cassandra table.
     *
     * @return List of column names.
     */
    private List<String> getPartitionKeyColumns() {
        List<String> cols = new LinkedList<>();

        if (PersistenceStrategy.BLOB == getStrategy() || PersistenceStrategy.PRIMITIVE == getStrategy()) {
            cols.add(getColumn());
            return cols;
        }

        if (partKeyFields != null) {
            for (PojoField field : partKeyFields)
                cols.add(field.getColumn());
        }

        return cols;
    }

    /**
     * Returns cluster key columns of Cassandra table.
     *
     * @return List of column names.
     */
    private List<String> getClusterKeyColumns() {
        List<String> cols = new LinkedList<>();

        if (clusterKeyFields != null) {
            for (PojoField field : clusterKeyFields)
                cols.add(field.getColumn());
        }

        return cols;
    }

    /**
     * Extracts POJO fields specified in XML element.
     *
     * @param el XML element describing fields.
     * @param descriptors POJO fields descriptors.
     * @return List of {@code This} fields.
     */
    private List<PojoField> detectFields(Element el, List<PropertyDescriptor> descriptors) {
        List<PojoField> list = new LinkedList<>();

        if (el == null && (descriptors == null || descriptors.isEmpty()))
            return list;

        if (el == null) {
            for (PropertyDescriptor desc : descriptors) {
                boolean valid = desc.getWriteMethod() != null ||
                        desc.getReadMethod().getAnnotation(QuerySqlField.class) != null ||
                        desc.getReadMethod().getAnnotation(AffinityKeyMapped.class) != null;

                // Skip POJO field if it's read-only and is not annotated with @QuerySqlField or @AffinityKeyMapped.
                if (valid)
                    list.add(new PojoKeyField(desc));
            }

            return list;
        }

        NodeList nodes = el.getElementsByTagName(FIELD_ELEMENT);

        int cnt = nodes == null ? 0 : nodes.getLength();

        if (cnt == 0) {
            throw new IllegalArgumentException("Incorrect configuration of Cassandra key persistence settings, " +
                "no key fields specified inside '" + PARTITION_KEY_ELEMENT + "/" +
                CLUSTER_KEY_ELEMENT + "' element");
        }

        for (int i = 0; i < cnt; i++) {
            PojoKeyField field = new PojoKeyField((Element)nodes.item(i), getJavaClass());

            PropertyDescriptor desc = findPropertyDescriptor(descriptors, field.getName());

            if (desc == null) {
                throw new IllegalArgumentException("Specified POJO field '" + field.getName() +
                    "' doesn't exist in '" + getJavaClass().getName() + "' class");
            }

            list.add(field);
        }

        return list;
    }

    /**
     * @return POJO field descriptors for partition key.
     */
    private List<PropertyDescriptor> getPartitionKeyDescriptors() {
        List<PropertyDescriptor> primitivePropDescriptors = PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(),
            AffinityKeyMapped.class, true);

        primitivePropDescriptors = primitivePropDescriptors != null && !primitivePropDescriptors.isEmpty() ?
            primitivePropDescriptors : PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(), true);

        boolean valid = false;

        for (PropertyDescriptor desc : primitivePropDescriptors) {
            if (desc.getWriteMethod() != null) {
                valid = true;

                break;
            }
        }

        if (!valid) {
            throw new IgniteException("Partition key can't have only calculated read-only fields, there should be " +
                    "some fields with setter method");
        }

        return primitivePropDescriptors;
    }

    /**
     * @return POJO field descriptors for cluster key.
     */
    private List<PropertyDescriptor> getClusterKeyDescriptors(List<PojoField> partKeyFields) {
        List<PropertyDescriptor> primitivePropDescriptors =
            PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(), true);

        if (primitivePropDescriptors == null || primitivePropDescriptors.isEmpty() ||
            partKeyFields.size() == primitivePropDescriptors.size())
            return null;

        for (PojoField field : partKeyFields) {
            for (int i = 0; i < primitivePropDescriptors.size(); i++) {
                if (primitivePropDescriptors.get(i).getName().equals(field.getName())) {
                    primitivePropDescriptors.remove(i);
                    break;
                }
            }
        }

        return primitivePropDescriptors;
    }
}
