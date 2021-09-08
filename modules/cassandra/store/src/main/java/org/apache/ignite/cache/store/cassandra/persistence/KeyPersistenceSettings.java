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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Stores persistence settings for Ignite cache key
 */
public class KeyPersistenceSettings extends PersistenceSettings<PojoKeyField> {
    /** Partition key XML tag. */
    private static final String PARTITION_KEY_ELEMENT = "partitionKey";

    /** Cluster key XML tag. */
    private static final String CLUSTER_KEY_ELEMENT = "clusterKey";

    /** POJO field XML tag. */
    private static final String FIELD_ELEMENT = "field";

    /** POJO fields. */
    private List<PojoKeyField> fields = new LinkedList<>();

    /** Partition key fields. */
    private List<PojoKeyField> partKeyFields = new LinkedList<>();

    /** Cluster key fields. */
    private List<PojoKeyField> clusterKeyFields = new LinkedList<>();

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

        Element node = el.getElementsByTagName(PARTITION_KEY_ELEMENT) != null ?
                (Element)el.getElementsByTagName(PARTITION_KEY_ELEMENT).item(0) : null;

        NodeList partKeysNodes = node == null ? null : node.getElementsByTagName(FIELD_ELEMENT);

        node = el.getElementsByTagName(CLUSTER_KEY_ELEMENT) != null ?
                (Element)el.getElementsByTagName(CLUSTER_KEY_ELEMENT).item(0) : null;

        NodeList clusterKeysNodes = node == null ? null : node.getElementsByTagName(FIELD_ELEMENT);

        if ((partKeysNodes == null || partKeysNodes.getLength() == 0) &&
                clusterKeysNodes != null && clusterKeysNodes.getLength() > 0) {
            throw new IllegalArgumentException("It's not allowed to specify cluster key fields mapping, but " +
                "doesn't specify partition key mappings");
        }

        // Detecting partition key fields
        partKeyFields = detectPojoFields(partKeysNodes);

        if (partKeyFields == null || partKeyFields.isEmpty()) {
            throw new IllegalStateException("Failed to initialize partition key fields for class '" +
                    getJavaClass().getName() + "'");
        }

        List<PojoKeyField> filteredFields = new LinkedList<>();

        // Find all fields annotated by @AffinityKeyMapped
        for (PojoKeyField field : partKeyFields) {
            if (field.getAnnotation(AffinityKeyMapped.class) != null)
                filteredFields.add(field);
        }

        // If there are any fields annotated by @AffinityKeyMapped then all other fields are part of cluster key
        partKeyFields = !filteredFields.isEmpty() ? filteredFields : partKeyFields;

        // Detecting cluster key fields
        clusterKeyFields = detectPojoFields(clusterKeysNodes);

        filteredFields = new LinkedList<>();

        // Removing out all fields which are already in partition key fields list
        for (PojoKeyField field : clusterKeyFields) {
            if (!PojoField.containsField(partKeyFields, field.getName()))
                filteredFields.add(field);
        }

        clusterKeyFields = filteredFields;

        fields = new LinkedList<>();
        fields.addAll(partKeyFields);
        fields.addAll(clusterKeyFields);

        checkDuplicates(fields);

        init();
    }

    /** {@inheritDoc} */
    @Override public List<PojoKeyField> getFields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override protected PojoKeyField createPojoField(Element el, Class clazz) {
        return new PojoKeyField(el, clazz);
    }

    /** {@inheritDoc} */
    @Override protected PojoKeyField createPojoField(PojoFieldAccessor accessor) {
        return new PojoKeyField(accessor);
    }

    /** {@inheritDoc} */
    @Override protected PojoKeyField createPojoField(PojoKeyField field, Class clazz) {
        return new PojoKeyField(field, clazz);
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
     * @see java.io.Serializable
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        fields = enrichFields(fields);
    }
}
