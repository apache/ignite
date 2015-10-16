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
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.store.cassandra.utils.common.PropertyMappingHelper;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Stores persistence settings for Ignite cache key
 */
public class KeyPersistenceSettings extends PersistenceSettings {
    private static final String PARTITION_KEY_ELEMENT = "partitionKey";
    private static final String CLUSTER_KEY_ELEMENT = "clusterKey";
    private static final String FIELD_ELEMENT = "field";

    private List<PojoField> fields = new LinkedList<>();
    private List<PojoField> partitionKeyFields = new LinkedList<>();
    private List<PojoField> clusterKeyFields = new LinkedList<>();

    public KeyPersistenceSettings(Element el) {
        super(el);

        if (!PersistenceStrategy.POJO.equals(getStrategy()))
            return;

        Element partitionKeysNode = el.getElementsByTagName(PARTITION_KEY_ELEMENT) != null ?
            (Element)el.getElementsByTagName(PARTITION_KEY_ELEMENT).item(0) : null;

        Element clusterKeysNode = el.getElementsByTagName(CLUSTER_KEY_ELEMENT) != null ?
            (Element)el.getElementsByTagName(CLUSTER_KEY_ELEMENT).item(0) : null;

        if (partitionKeysNode == null && clusterKeysNode != null) {
            throw new IllegalArgumentException("It's not allowed to specify cluster key fields mapping, but " +
                "doesn't specify partition key mappings");
        }

        partitionKeyFields = detectFields(partitionKeysNode, getPartitionKeyDescriptors());

        if (partitionKeyFields == null || partitionKeyFields.isEmpty()) {
            throw new IllegalStateException("Failed to initialize partition key fields for class '" +
                getJavaClass().getName() + "'");
        }

        clusterKeyFields = detectFields(clusterKeysNode, getClusterKeyDescriptors(partitionKeyFields));

        fields = new LinkedList<>();
        fields.addAll(partitionKeyFields);
        fields.addAll(clusterKeyFields);

        checkDuplicates(fields);
    }

    @Override public List<PojoField> getFields() {
        return fields;
    }

    public String getPrimaryKeyDDL() {
        StringBuilder partitionKey = new StringBuilder();

        List<String> columns = getPartitionKeyColumns();
        for (String column : columns) {
            if (partitionKey.length() != 0)
                partitionKey.append(", ");

            partitionKey.append(column);
        }

        StringBuilder clusterKey = new StringBuilder();

        columns = getClusterKeyColumns();
        if (columns != null) {
            for (String column : columns) {
                if (clusterKey.length() != 0)
                    clusterKey.append(", ");

                clusterKey.append(column);
            }
        }

        return clusterKey.length() == 0 ?
            "primary key ((" + partitionKey.toString() + "))" :
            "primary key ((" + partitionKey.toString() + "), " + clusterKey.toString() + ")";
    }

    public String getClusteringDDL() {
        StringBuilder builder = new StringBuilder();

        for (PojoField field : clusterKeyFields) {
            PojoKeyField.SortOrder sortOrder = ((PojoKeyField)field).getSortOrder();

            if (sortOrder == null)
                continue;

            if (builder.length() != 0)
                builder.append(", ");

            boolean asc = PojoKeyField.SortOrder.ASC.equals(sortOrder);

            builder.append(field.getColumn()).append(" ").append(asc ? "asc" : "desc");
        }

        return builder.length() == 0 ? null : "clustering order by (" + builder.toString() + ")";
    }

    @Override protected String defaultColumnName() {
        return "key";
    }

    private List<String> getPartitionKeyColumns() {
        List<String> columns = new LinkedList<>();

        if (PersistenceStrategy.BLOB.equals(getStrategy()) || PersistenceStrategy.PRIMITIVE.equals(getStrategy())) {
            columns.add(getColumn());
            return columns;
        }

        if (partitionKeyFields != null) {
            for (PojoField field : partitionKeyFields)
                columns.add(field.getColumn());
        }

        return columns;
    }

    private List<String> getClusterKeyColumns() {
        List<String> columns = new LinkedList<>();

        if (clusterKeyFields != null) {
            for (PojoField field : clusterKeyFields)
                columns.add(field.getColumn());
        }

        return columns;
    }

    private List<PojoField> detectFields(Element el, List<PropertyDescriptor> descriptors) {
        List<PojoField> list = new LinkedList<>();

        if (el == null && (descriptors == null || descriptors.isEmpty()))
            return list;

        if (el == null) {
            for (PropertyDescriptor descriptor : descriptors)
                list.add(new PojoKeyField(descriptor));

            return list;
        }

        NodeList nodes = el.getElementsByTagName(FIELD_ELEMENT);
        int count = nodes == null ? 0 : nodes.getLength();

        if (count == 0) {
            throw new IllegalArgumentException("Incorrect configuration of Cassandra key persistence settings, " +
                "no cluster key fields specified inside '" + PARTITION_KEY_ELEMENT + "/" +
                CLUSTER_KEY_ELEMENT + "' element");
        }

        for (int i = 0; i < count; i++) {
            PojoKeyField field = new PojoKeyField((Element)nodes.item(i), getJavaClass());

            PropertyDescriptor descriptor = findPropertyDescriptor(descriptors, field.getName());

            if (descriptor == null) {
                throw new IllegalArgumentException("Specified POJO field '" + field.getName() +
                    "' doesn't exist in '" + getJavaClass().getName() + "' class");
            }

            list.add(field);
        }

        return list;
    }

    private List<PropertyDescriptor> getPartitionKeyDescriptors() {
        List<PropertyDescriptor> primitivePropDescriptors = PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(),
            AffinityKeyMapped.class, true);

        return primitivePropDescriptors != null && !primitivePropDescriptors.isEmpty() ?
            primitivePropDescriptors :
            PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(), true);
    }

    private List<PropertyDescriptor> getClusterKeyDescriptors(List<PojoField> partitionKeyFields) {
        List<PropertyDescriptor> primitivePropDescriptors =
            PropertyMappingHelper.getPojoPropertyDescriptors(getJavaClass(), true);

        if (primitivePropDescriptors == null || primitivePropDescriptors.isEmpty() ||
            partitionKeyFields.size() == primitivePropDescriptors.size()) {
            return null;
        }

        for (PojoField field : partitionKeyFields) {
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
