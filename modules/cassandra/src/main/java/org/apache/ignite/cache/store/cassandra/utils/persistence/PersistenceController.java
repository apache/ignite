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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.utils.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.utils.serializer.Serializer;

/**
 * Intermediate layer between persistent store (Cassandra) and Ignite cache key/value classes. Handles
 * all the mappings to/from Java classes into Cassandra and responsible for all the details of how Java
 * objects should be written/loaded  to/from Cassandra
 */
public class PersistenceController {
    private KeyValuePersistenceSettings persistenceSettings;

    private String writeStatement;
    private String deleteStatement;
    private String loadStatement;
    private String loadStatementWithKeyFields;

    public PersistenceController(KeyValuePersistenceSettings settings) {
        if (settings == null)
            throw new IllegalArgumentException("Persistent settings can't be null");

        this.persistenceSettings = settings;
    }

    public KeyValuePersistenceSettings getPersistenceSettings() {
        return persistenceSettings;
    }

    public String getKeyspace() {
        return persistenceSettings.getKeyspace();
    }

    public String getTable() {
        return persistenceSettings.getTable();
    }

    public String getWriteStatement() {
        if (writeStatement != null) {
            return writeStatement;
        }

        List<String> columns = getKeyValueColumns();

        StringBuilder columnsList = new StringBuilder();
        StringBuilder questionsList = new StringBuilder();

        for (String column : columns) {
            if (columnsList.length() != 0) {
                columnsList.append(", ");
                questionsList.append(",");
            }

            columnsList.append(column);
            questionsList.append("?");
        }

        writeStatement = "insert into " + persistenceSettings.getKeyspace() + "." + persistenceSettings.getTable() + " (" +
            columnsList.toString() + ") values (" + questionsList.toString() + ")";

        if (persistenceSettings.getTTL() != null)
            writeStatement += " using ttl " + persistenceSettings.getTTL();

        writeStatement += ";";

        return writeStatement;
    }

    public String getDeleteStatement() {
        if (deleteStatement != null) {
            return deleteStatement;
        }

        List<String> columns = getKeyColumns();

        StringBuilder statement = new StringBuilder();

        for (String column : columns) {
            if (statement.length() != 0)
                statement.append(" and ");

            statement.append(column).append("=?");
        }

        statement.append(";");

        deleteStatement = "delete from " +
            persistenceSettings.getKeyspace() + "." +
            persistenceSettings.getTable() + " where " +
            statement.toString();

        return deleteStatement;
    }

    public String getLoadStatement(boolean includeKeyFields) {
        if (loadStatement != null && loadStatementWithKeyFields != null)
            return includeKeyFields ? loadStatementWithKeyFields : loadStatement;

        List<String> valColumns = getValueColumns();
        List<String> keyColumns = getKeyColumns();

        StringBuilder headerWithKeyFields = new StringBuilder("select ");

        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0)
                headerWithKeyFields.append(", ");

            headerWithKeyFields.append(keyColumns.get(i));
        }

        StringBuilder header = new StringBuilder("select ");

        for (int i = 0; i < valColumns.size(); i++) {
            if (i > 0)
                header.append(", ");

            headerWithKeyFields.append(",");

            header.append(valColumns.get(i));
            headerWithKeyFields.append(valColumns.get(i));
        }

        StringBuilder statement = new StringBuilder();

        statement.append(" from ");
        statement.append(persistenceSettings.getKeyspace());
        statement.append(".").append(persistenceSettings.getTable());
        statement.append(" where ");

        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0)
                statement.append(" and ");

            statement.append(keyColumns.get(i)).append("=?");
        }

        statement.append(";");

        loadStatement = header.toString() + statement.toString();
        loadStatementWithKeyFields = headerWithKeyFields.toString() + statement.toString();

        return includeKeyFields ? loadStatementWithKeyFields : loadStatement;
    }

    public BoundStatement bindKey(PreparedStatement statement, Object key) {
        KeyPersistenceSettings settings = persistenceSettings.getKeyPersistenceSettings();

        Object[] values = getBindingValues(settings.getStrategy(),
            settings.getSerializer(), settings.getFields(), key);

        return statement.bind(values);
    }

    public BoundStatement bindKeyValue(PreparedStatement statement, Object key, Object value) {
        KeyPersistenceSettings keySettings = persistenceSettings.getKeyPersistenceSettings();
        Object[] keyValues = getBindingValues(keySettings.getStrategy(),
            keySettings.getSerializer(), keySettings.getFields(), key);

        ValuePersistenceSettings valSettings = persistenceSettings.getValuePersistenceSettings();
        Object[] valValues = getBindingValues(valSettings.getStrategy(),
            valSettings.getSerializer(), valSettings.getFields(), value);

        Object[] values = new Object[keyValues.length + valValues.length];

        int i = 0;

        for (Object val : keyValues) {
            values[i] = val;
            i++;
        }

        for (Object val : valValues) {
            values[i] = val;
            i++;
        }

        return statement.bind(values);
    }

    @SuppressWarnings("UnusedDeclaration")
    public Object buildKeyObject(Row row) {
        return buildObject(row, persistenceSettings.getKeyPersistenceSettings());
    }

    public Object buildValueObject(Row row) {
        return buildObject(row, persistenceSettings.getValuePersistenceSettings());
    }

    private Object buildObject(Row row, PersistenceSettings settings) {
        if (row == null)
            return null;

        PersistenceStrategy strategy = settings.getStrategy();
        Class clazz = settings.getJavaClass();
        String column = settings.getColumn();
        List<PojoField> fields = settings.getFields();

        if (PersistenceStrategy.PRIMITIVE.equals(strategy))
            return PropertyMappingHelper.getCassandraColumnValue(row, column, clazz, null);

        if (PersistenceStrategy.BLOB.equals(strategy))
            return settings.getSerializer().deserialize(row.getBytes(column));

        Object obj;

        try {
            obj = clazz.newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate object of type '" + clazz.getName() + "' using reflection", e);
        }

        for (PojoField field : fields)
            field.setValueFromRow(row, obj, settings.getSerializer());

        return obj;
    }

    private Object[] getBindingValues(PersistenceStrategy strategy, Serializer serializer,
        List<PojoField> fields, Object obj) {
        if (PersistenceStrategy.PRIMITIVE.equals(strategy)) {
            if (PropertyMappingHelper.getCassandraType(obj.getClass()) == null ||
                obj.getClass().equals(ByteBuffer.class) || obj instanceof byte[]) {
                throw new IllegalArgumentException("Couldn't deserialize instance of class '" +
                    obj.getClass().getName() + "' using PRIMITIVE strategy. Please use BLOB strategy for this case.");
            }

            return new Object[] {obj};
        }

        if (PersistenceStrategy.BLOB.equals(strategy))
            return new Object[] {serializer.serialize(obj)};

        Object[] values = new Object[fields.size()];

        int i = 0;

        for (PojoField field : fields) {
            Object value = field.getValueFromObject(obj, serializer);

            if (value instanceof byte[])
                value = ByteBuffer.wrap((byte[])value);

            values[i] = value;

            i++;
        }

        return values;
    }

    private List<String> getKeyValueColumns() {
        List<String> columns = getKeyColumns();
        columns.addAll(getValueColumns());
        return columns;
    }

    private List<String> getKeyColumns() {
        return getColumns(persistenceSettings.getKeyPersistenceSettings());
    }

    private List<String> getValueColumns() {
        return getColumns(persistenceSettings.getValuePersistenceSettings());
    }

    private List<String> getColumns(PersistenceSettings settings) {
        List<String> columns = new LinkedList<>();

        if (!PersistenceStrategy.POJO.equals(settings.getStrategy())) {
            columns.add(settings.getColumn());
            return columns;
        }

        for (PojoField field : settings.getFields())
            columns.add(field.getColumn());

        return columns;
    }
}
