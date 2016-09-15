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
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

/**
 * Intermediate layer between persistent store (Cassandra) and Ignite cache key/value classes.
 * Handles  all the mappings to/from Java classes into Cassandra and responsible for all the details
 * of how Java objects should be written/loaded to/from Cassandra.
 */
public class PersistenceController {
    /** Ignite cache key/value persistence settings. */
    private KeyValuePersistenceSettings persistenceSettings;

    /** CQL statement to insert row into Cassandra table. */
    private String writeStatement;

    /** CQL statement to delete row from Cassandra table. */
    private String delStatement;

    /** CQL statement to select value fields from Cassandra table. */
    private String loadStatement;

    /** CQL statement to select key/value fields from Cassandra table. */
    private String loadStatementWithKeyFields;

    /**
     * Constructs persistence controller from Ignite cache persistence settings.
     *
     * @param settings persistence settings.
     */
    public PersistenceController(KeyValuePersistenceSettings settings) {
        if (settings == null)
            throw new IllegalArgumentException("Persistent settings can't be null");

        this.persistenceSettings = settings;
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
     * Returns Cassandra keyspace to use.
     *
     * @return keyspace.
     */
    public String getKeyspace() {
        return persistenceSettings.getKeyspace();
    }

    /**
     * Returns Cassandra table to use.
     *
     * @return table.
     */
    public String getTable() {
        return persistenceSettings.getTable();
    }

    /**
     * Returns CQL statement to insert row into Cassandra table.
     *
     * @return CQL statement.
     */
    public String getWriteStatement() {
        if (writeStatement != null)
            return writeStatement;

        List<String> cols = getKeyValueColumns();

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

        writeStatement = "insert into \"" + persistenceSettings.getKeyspace() + "\".\"" + persistenceSettings.getTable() + "\" (" +
            colsList.toString() + ") values (" + questionsList.toString() + ")";

        if (persistenceSettings.getTTL() != null)
            writeStatement += " using ttl " + persistenceSettings.getTTL();

        writeStatement += ";";

        return writeStatement;
    }

    /**
     * Returns CQL statement to delete row from Cassandra table.
     *
     * @return CQL statement.
     */
    public String getDeleteStatement() {
        if (delStatement != null)
            return delStatement;

        List<String> cols = getKeyColumns();

        StringBuilder statement = new StringBuilder();

        for (String column : cols) {
            if (statement.length() != 0)
                statement.append(" and ");

            statement.append("\"").append(column).append("\"=?");
        }

        statement.append(";");

        delStatement = "delete from \"" +
            persistenceSettings.getKeyspace() + "\".\"" +
            persistenceSettings.getTable() + "\" where " +
            statement.toString();

        return delStatement;
    }

    /**
     * Returns CQL statement to select key/value fields from Cassandra table.
     *
     * @param includeKeyFields whether to include/exclude key fields from the returned row.
     *
     * @return CQL statement.
     */
    public String getLoadStatement(boolean includeKeyFields) {
        if (loadStatement != null && loadStatementWithKeyFields != null)
            return includeKeyFields ? loadStatementWithKeyFields : loadStatement;

        List<String> valCols = getValueColumns();

        List<String> keyCols = getKeyColumns();

        StringBuilder hdrWithKeyFields = new StringBuilder("select ");

        for (int i = 0; i < keyCols.size(); i++) {
            if (i > 0)
                hdrWithKeyFields.append(", ");

            hdrWithKeyFields.append("\"").append(keyCols.get(i)).append("\"");
        }

        StringBuilder hdr = new StringBuilder("select ");

        for (int i = 0; i < valCols.size(); i++) {
            if (i > 0)
                hdr.append(", ");

            hdrWithKeyFields.append(",");

            hdr.append("\"").append(valCols.get(i)).append("\"");
            hdrWithKeyFields.append("\"").append(valCols.get(i)).append("\"");
        }

        StringBuilder statement = new StringBuilder();

        statement.append(" from \"");
        statement.append(persistenceSettings.getKeyspace());
        statement.append("\".\"").append(persistenceSettings.getTable());
        statement.append("\" where ");

        for (int i = 0; i < keyCols.size(); i++) {
            if (i > 0)
                statement.append(" and ");

            statement.append("\"").append(keyCols.get(i)).append("\"=?");
        }

        statement.append(";");

        loadStatement = hdr.toString() + statement.toString();
        loadStatementWithKeyFields = hdrWithKeyFields.toString() + statement.toString();

        return includeKeyFields ? loadStatementWithKeyFields : loadStatement;
    }

    /**
     * Binds Ignite cache key object to {@link com.datastax.driver.core.PreparedStatement}.
     *
     * @param statement statement to which key object should be bind.
     * @param key key object.
     *
     * @return statement with bounded key.
     */
    public BoundStatement bindKey(PreparedStatement statement, Object key) {
        KeyPersistenceSettings settings = persistenceSettings.getKeyPersistenceSettings();

        Object[] values = getBindingValues(settings.getStrategy(),
            settings.getSerializer(), settings.getFields(), key);

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
        KeyPersistenceSettings keySettings = persistenceSettings.getKeyPersistenceSettings();
        Object[] keyValues = getBindingValues(keySettings.getStrategy(),
            keySettings.getSerializer(), keySettings.getFields(), key);

        ValuePersistenceSettings valSettings = persistenceSettings.getValuePersistenceSettings();
        Object[] valValues = getBindingValues(valSettings.getStrategy(),
            valSettings.getSerializer(), valSettings.getFields(), val);

        Object[] values = new Object[keyValues.length + valValues.length];

        int i = 0;

        for (Object keyVal : keyValues) {
            values[i] = keyVal;
            i++;
        }

        for (Object valVal : valValues) {
            values[i] = valVal;
            i++;
        }

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

        PersistenceStrategy stgy = settings.getStrategy();

        Class clazz = settings.getJavaClass();

        String col = settings.getColumn();

        List<PojoField> fields = settings.getFields();

        if (PersistenceStrategy.PRIMITIVE.equals(stgy))
            return PropertyMappingHelper.getCassandraColumnValue(row, col, clazz, null);

        if (PersistenceStrategy.BLOB.equals(stgy))
            return settings.getSerializer().deserialize(row.getBytes(col));

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

    /**
     * Extracts field values from POJO object and converts them into Java types
     * which could be mapped to Cassandra types.
     *
     * @param stgy persistence strategy to use.
     * @param serializer serializer to use for BLOBs.
     * @param fields fields who's values should be extracted.
     * @param obj object instance who's field values should be extracted.
     *
     * @return array of object field values converted into Java object instances having Cassandra compatible types
     */
    private Object[] getBindingValues(PersistenceStrategy stgy, Serializer serializer, List<PojoField> fields, Object obj) {
        if (PersistenceStrategy.PRIMITIVE.equals(stgy)) {
            if (PropertyMappingHelper.getCassandraType(obj.getClass()) == null ||
                obj.getClass().equals(ByteBuffer.class) || obj instanceof byte[]) {
                throw new IllegalArgumentException("Couldn't deserialize instance of class '" +
                    obj.getClass().getName() + "' using PRIMITIVE strategy. Please use BLOB strategy for this case.");
            }

            return new Object[] {obj};
        }

        if (PersistenceStrategy.BLOB.equals(stgy))
            return new Object[] {serializer.serialize(obj)};

        Object[] values = new Object[fields.size()];

        int i = 0;

        for (PojoField field : fields) {
            Object val = field.getValueFromObject(obj, serializer);

            if (val instanceof byte[])
                val = ByteBuffer.wrap((byte[]) val);

            values[i] = val;

            i++;
        }

        return values;
    }

    /**
     * Returns list of Cassandra table columns mapped to Ignite cache key and value fields
     *
     * @return list of column names
     */
    private List<String> getKeyValueColumns() {
        List<String> cols = getKeyColumns();

        cols.addAll(getValueColumns());

        return cols;
    }

    /**
     * Returns list of Cassandra table columns mapped to Ignite cache key fields
     *
     * @return list of column names
     */
    private List<String> getKeyColumns() {
        return getColumns(persistenceSettings.getKeyPersistenceSettings());
    }

    /**
     * Returns list of Cassandra table columns mapped to Ignite cache value fields
     *
     * @return list of column names
     */
    private List<String> getValueColumns() {
        return getColumns(persistenceSettings.getValuePersistenceSettings());
    }

    /**
     * Returns list of Cassandra table columns based on persistence strategy to use
     *
     * @return list of column names
     */
    private List<String> getColumns(PersistenceSettings settings) {
        List<String> cols = new LinkedList<>();

        if (!PersistenceStrategy.POJO.equals(settings.getStrategy())) {
            cols.add(settings.getColumn());
            return cols;
        }

        for (PojoField field : settings.getFields())
            cols.add(field.getColumn());

        return cols;
    }
}
