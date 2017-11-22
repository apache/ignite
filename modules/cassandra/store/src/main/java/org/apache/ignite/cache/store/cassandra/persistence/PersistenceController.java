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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.common.TypeHandler;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

/**
 * Intermediate layer between persistent store (Cassandra) and Ignite cache key/value classes.
 * Handles  all the mappings to/from Java classes into Cassandra and responsible for all the details
 * of how Java objects should be written/loaded to/from Cassandra.
 */
public class PersistenceController {
    /** Ignite cache key/value persistence settings. */
    private final KeyValuePersistenceSettings persistenceSettings;

    /** List of key unique POJO fields (skipping aliases pointing to the same Cassandra table column). */
    private final List<PojoField> keyUniquePojoFields;

    /** List of value unique POJO fields (skipping aliases pointing to the same Cassandra table column). */
    private final List<PojoField> valUniquePojoFields;

    /** CQL statement template to insert row into Cassandra table. */
    private final String writeStatementTempl;

    /** CQL statement template to delete row from Cassandra table. */
    private final String delStatementTempl;

    /** CQL statement template to select value fields from Cassandra table. */
    private final String loadStatementTempl;

    /** CQL statement template to select key/value fields from Cassandra table. */
    private final String loadWithKeyFieldsStatementTempl;

    /** CQL statements to insert row into Cassandra table. */
    private volatile Map<String, String> writeStatements = new HashMap<>();

    /** CQL statements to delete row from Cassandra table. */
    private volatile Map<String, String> delStatements = new HashMap<>();

    /** CQL statements to select value fields from Cassandra table. */
    private volatile Map<String, String> loadStatements = new HashMap<>();

    /** CQL statements to select key/value fields from Cassandra table. */
    private volatile Map<String, String> loadWithKeyFieldsStatements = new HashMap<>();

    /**
     * Constructs persistence controller from Ignite cache persistence settings.
     *
     * @param settings persistence settings.
     */
    public PersistenceController(KeyValuePersistenceSettings settings) {
        if (settings == null)
            throw new IllegalArgumentException("Persistent settings can't be null");

        persistenceSettings = settings;

        String[] loadStatements = prepareLoadStatements();

        loadWithKeyFieldsStatementTempl = loadStatements[0];
        loadStatementTempl = loadStatements[1];
        writeStatementTempl = prepareWriteStatement();
        delStatementTempl = prepareDeleteStatement();

        keyUniquePojoFields = settings.getKeyPersistenceSettings().cassandraUniqueFields();

        List<PojoField> _valUniquePojoFields = settings.getValuePersistenceSettings().cassandraUniqueFields();

        if (_valUniquePojoFields == null || _valUniquePojoFields.isEmpty()) {
            valUniquePojoFields = _valUniquePojoFields;

            return;
        }

        List<String> keyColumns = new LinkedList<>();

        if (keyUniquePojoFields == null)
            keyColumns.add(settings.getKeyPersistenceSettings().getColumn());
        else {
            for (PojoField field : keyUniquePojoFields)
                keyColumns.add(field.getColumn());
        }

        List<PojoField> fields = new LinkedList<>(_valUniquePojoFields);

        for (String column : keyColumns) {
            for (int i = 0; i < fields.size(); i++) {
                if (column.equals(fields.get(i).getColumn())) {
                    fields.remove(i);
                    break;
                }
            }
        }

        valUniquePojoFields = fields.isEmpty() ? null : Collections.unmodifiableList(fields);
    }

    /**
     * Returns Ignite cache persistence settings.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings() {
        return persistenceSettings;
    }

    /**
     * Returns CQL statement to insert row into Cassandra table.
     *
     * @param table Table name.
     * @return CQL statement.
     */
    public String getWriteStatement(String table) {
        return getStatement(table, writeStatementTempl, writeStatements);
    }

    /**
     * Returns CQL statement to delete row from Cassandra table.
     *
     * @param table Table name.
     * @return CQL statement.
     */
    public String getDeleteStatement(String table) {
        return getStatement(table, delStatementTempl, delStatements);
    }

    /**
     * Returns CQL statement to select key/value fields from Cassandra table.
     *
     * @param table Table name.
     * @param includeKeyFields whether to include/exclude key fields from the returned row.
     *
     * @return CQL statement.
     */
    public String getLoadStatement(String table, boolean includeKeyFields) {
        return includeKeyFields ?
            getStatement(table, loadWithKeyFieldsStatementTempl, loadWithKeyFieldsStatements) :
            getStatement(table, loadStatementTempl, loadStatements);
    }

    /**
     * Binds Ignite cache key object to {@link PreparedStatement}.
     *
     * @param statement statement to which key object should be bind.
     * @param key key object.
     *
     * @return statement with bounded key.
     */
    public BoundStatement bindKey(PreparedStatement statement, Object key) {
        PersistenceSettings settings = persistenceSettings.getKeyPersistenceSettings();

        Object[] values = PersistenceStrategy.POJO != settings.getStrategy() ?
            new Object[1] : new Object[keyUniquePojoFields.size()];

        bindValues(settings.getStrategy(), settings.getTypeHandler(), settings.getSerializer(), keyUniquePojoFields, key, values, 0);

        return statement.bind(values);
    }

    /**
     * Binds Ignite cache key and value object to {@link com.datastax.driver.core.PreparedStatement}.
     *
     * @param statement statement to which key and value object should be bind.
     * @param key key object.
     * @param val value object.
     *
     * @return statement with bounded key and value.
     */
    public BoundStatement bindKeyValue(PreparedStatement statement, Object key, Object val) {
        Object[] values = new Object[persistenceSettings.getTableColumns().size()];

        PersistenceSettings keySettings = persistenceSettings.getKeyPersistenceSettings();
        PersistenceSettings valSettings = persistenceSettings.getValuePersistenceSettings();

        int offset = bindValues(keySettings.getStrategy(), keySettings.getTypeHandler(), keySettings.getSerializer(), keyUniquePojoFields, key, values, 0);
        bindValues(valSettings.getStrategy(), valSettings.getTypeHandler(), valSettings.getSerializer(), valUniquePojoFields, val, values, offset);

        return statement.bind(values);
    }

    /**
     * Builds Ignite cache key object from returned Cassandra table row.
     *
     * @param row Cassandra table row.
     *
     * @return key object.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Object buildKeyObject(Row row) {
        return buildObject(row, persistenceSettings.getKeyPersistenceSettings());
    }

    /**
     * Builds Ignite cache value object from Cassandra table row .
     *
     * @param row Cassandra table row.
     *
     * @return value object.
     */
    public Object buildValueObject(Row row) {
        return buildObject(row, persistenceSettings.getValuePersistenceSettings());
    }

    /**
     * Service method to prepare CQL write statement.
     *
     * @return CQL write statement.
     */
    private String prepareWriteStatement() {
        Collection<String> cols = persistenceSettings.getTableColumns();

        StringBuilder colsList = new StringBuilder();
        StringBuilder questionsList = new StringBuilder();

        for (String column : cols) {
            if (colsList.length() != 0) {
                colsList.append(", ");
                questionsList.append(",");
            }

            colsList.append("\"").append(column).append("\"");
            questionsList.append("?");
        }

        String statement = "insert into \"" + persistenceSettings.getKeyspace() + "\".\"%1$s" +
            "\" (" + colsList + ") values (" + questionsList + ")";

        if (persistenceSettings.getTTL() != null)
            statement += " using ttl " + persistenceSettings.getTTL();

        return statement + ";";
    }

    /**
     * Service method to prepare CQL delete statement.
     *
     * @return CQL write statement.
     */
    private String prepareDeleteStatement() {
        Collection<String> cols = persistenceSettings.getKeyPersistenceSettings().getTableColumns();

        StringBuilder statement = new StringBuilder();

        for (String column : cols) {
            if (statement.length() != 0)
                statement.append(" and ");

            statement.append("\"").append(column).append("\"=?");
        }

        statement.append(";");

        return "delete from \"" + persistenceSettings.getKeyspace() + "\".\"%1$s\" where " + statement;
    }

    /**
     * Service method to prepare CQL load statements including and excluding key columns.
     *
     * @return array having two CQL statements (including and excluding key columns).
     */
    private String[] prepareLoadStatements() {
        PersistenceSettings settings = persistenceSettings.getKeyPersistenceSettings();
        boolean pojoStrategy = PersistenceStrategy.POJO == settings.getStrategy();
        Collection<String> keyCols = settings.getTableColumns();
        StringBuilder hdrWithKeyFields = new StringBuilder();


        for (String column : keyCols) {
            // omit calculated fields in load statement
            if (pojoStrategy && settings.getFieldByColumn(column).calculatedField())
                continue;

            if (hdrWithKeyFields.length() > 0)
                hdrWithKeyFields.append(", ");

            hdrWithKeyFields.append("\"").append(column).append("\"");
        }

        settings = persistenceSettings.getValuePersistenceSettings();
        pojoStrategy = PersistenceStrategy.POJO == settings.getStrategy();
        Collection<String> valCols = settings.getTableColumns();
        StringBuilder hdr = new StringBuilder();

        for (String column : valCols) {
            // omit calculated fields in load statement
            if (pojoStrategy && settings.getFieldByColumn(column).calculatedField())
                continue;

            if (hdr.length() > 0)
                hdr.append(", ");

            hdr.append("\"").append(column).append("\"");

            if (!keyCols.contains(column))
                hdrWithKeyFields.append(", \"").append(column).append("\"");
        }

        hdrWithKeyFields.insert(0, "select ");
        hdr.insert(0, "select ");

        StringBuilder statement = new StringBuilder();

        statement.append(" from \"");
        statement.append(persistenceSettings.getKeyspace());
        statement.append("\".\"%1$s");
        statement.append("\" where ");

        int i = 0;

        for (String column : keyCols) {
            if (i > 0)
                statement.append(" and ");

            statement.append("\"").append(column).append("\"=?");
            i++;
        }

        statement.append(";");

        return new String[] {hdrWithKeyFields + statement.toString(), hdr + statement.toString()};
    }

    /**
     * @param table Table.
     * @param template Template.
     * @param statements Statements.
     * @return Statement.
     */
    private String getStatement(final String table, final String template, final Map<String, String> statements) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (statements) {
            String st = statements.get(table);

            if (st == null) {
                st = String.format(template, table);
                statements.put(table, st);
            }

            return st;
        }
    }

    /**
     * Builds object from Cassandra table row.
     *
     * @param row Cassandra table row.
     * @param settings persistence settings to use.
     *
     * @return object.
     */
    private Object buildObject(Row row, PersistenceSettings settings) {
        if (row == null)
            return null;

        PersistenceStrategy stg = settings.getStrategy();

        Class clazz = settings.getJavaClass();
        String col = settings.getColumn();

        if (PersistenceStrategy.PRIMITIVE == stg)
            return PropertyMappingHelper.getCassandraColumnValue(row, col, clazz, null);

        if (PersistenceStrategy.BLOB == stg)
            return settings.getSerializer().deserialize(row.getBytes(col));

        List<PojoField> fields = settings.getFields();

        Object obj;

        try {
            obj = clazz.newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate object of type '" + clazz.getName() + "' using reflection", e);
        }

        for (PojoField field : fields) {
            if (!field.calculatedField())
                field.setValueFromRow(row, obj, settings.getSerializer());
        }

        return obj;
    }

    /**
     * Extracts field values from POJO object, converts into Java types
     * which could be mapped to Cassandra types and stores them inside provided values
     * array starting from specified offset.
     *
     * @param stgy Persistence strategy to use.
     * @param serializer Serializer to use for BLOBs.
     * @param fields Fields who's values should be extracted.
     * @param obj Object instance who's field values should be extracted.
     * @param values Array to store values.
     * @param offset Offset starting from which to store fields values in the provided values array.
     *
     * @return next offset
     */
    private int bindValues(PersistenceStrategy stgy, TypeHandler primitiveHandler, Serializer serializer, List<PojoField> fields, Object obj,
                           Object[] values, int offset) {
        if (PersistenceStrategy.PRIMITIVE == stgy) {
            if(primitiveHandler != null) {
                obj = primitiveHandler.toCassandraPrimitiveType(obj);
            } else if (PropertyMappingHelper.getCassandraType(obj.getClass()) == null ||
                obj.getClass().equals(ByteBuffer.class) || obj instanceof byte[]) {
                throw new IllegalArgumentException("Couldn't deserialize instance of class '" +
                    obj.getClass().getName() + "' using PRIMITIVE strategy. Please use BLOB strategy for this case.");
            }

            values[offset] = obj;

            return ++offset;
        }

        if (PersistenceStrategy.BLOB == stgy) {
            values[offset] = serializer.serialize(obj);

            return ++offset;
        }

        if (fields == null || fields.isEmpty())
            return offset;

        for (PojoField field : fields) {
            Object val = field.getValueFromObject(obj, serializer);

            if (val instanceof byte[])
                val = ByteBuffer.wrap((byte[]) val);

            values[offset] = val;

            offset++;
        }

        return offset;
    }
}
