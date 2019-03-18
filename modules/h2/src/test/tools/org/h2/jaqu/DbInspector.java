/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import org.h2.jaqu.Table.JQTable;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * Class to inspect a model and a database for the purposes of model validation
 * and automatic model generation. This class finds the available schemas and
 * tables and serves as the entry point for model generation and validation.
 */
public class DbInspector {

    private final Db db;
    private DatabaseMetaData metaData;
    private Class<? extends java.util.Date> dateTimeClass = java.util.Date.class;

    public DbInspector(Db db) {
        this.db = db;
    }

    /**
     * Set the preferred class to store date and time. Possible values are:
     * java.util.Date (default) and java.sql.Timestamp.
     *
     * @param dateTimeClass the new class
     */
    public void setPreferredDateTimeClass(
            Class<? extends java.util.Date> dateTimeClass) {
        this.dateTimeClass = dateTimeClass;
    }

    /**
     * Generates models class skeletons for schemas and tables. If the table
     * name is undefined, models will be generated for every table within the
     * specified schema. Additionally, if no schema is defined, models will be
     * generated for all schemas and all tables.
     *
     * @param schema the schema name (optional)
     * @param table the table name (optional)
     * @param packageName the package name (optional)
     * @param annotateSchema (includes schema name in annotation)
     * @param trimStrings (trims strings to maxLength of column)
     * @return a list of complete model classes as strings, each element a class
     */
    public List<String> generateModel(String schema, String table,
            String packageName, boolean annotateSchema, boolean trimStrings) {
        try {
            List<String> models = New.arrayList();
            List<TableInspector> tables = getTables(schema, table);
            for (TableInspector t : tables) {
                t.read(metaData);
                String model = t.generateModel(packageName, annotateSchema,
                        trimStrings);
                models.add(model);
            }
            return models;
        } catch (SQLException s) {
            throw new RuntimeException(s);
        }
    }

    /**
     * Validates a model.
     *
     * @param <T> the model class
     * @param model an instance of the model class
     * @param throwOnError if errors should cause validation to fail
     * @return a list of validation remarks
     */
    public <T> List<ValidationRemark> validateModel(T model,
            boolean throwOnError) {
        try {
            TableInspector inspector = getTable(model);
            inspector.read(metaData);
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) model.getClass();
            TableDefinition<T> def = db.define(clazz);
            return inspector.validate(def, throwOnError);
        } catch (SQLException s) {
            throw new RuntimeException(s);
        }
    }

    private DatabaseMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = db.getConnection().getMetaData();
        }
        return metaData;
    }

    /**
     * Get the table in the database based on the model definition.
     *
     * @param <T> the model class
     * @param model an instance of the model class
     * @return the table inspector
     */
    private <T> TableInspector getTable(T model) throws SQLException {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) model.getClass();
        TableDefinition<T> def = db.define(clazz);
        boolean forceUpperCase = getMetaData().storesUpperCaseIdentifiers();
        String schema = (forceUpperCase && def.schemaName != null) ? def.schemaName
                .toUpperCase() : def.schemaName;
        String table = forceUpperCase ? def.tableName.toUpperCase()
                : def.tableName;
        List<TableInspector> tables = getTables(schema, table);
        return tables.get(0);
    }

    /**
     * Returns a list of tables. This method always returns at least one
     * element. If no table is found, an exception is thrown.
     *
     * @param schema the schema name
     * @param table the table name
     * @return a list of table inspectors (always contains at least one element)
     */
    private List<TableInspector> getTables(String schema, String table)
            throws SQLException {
        ResultSet rs = null;
        try {
            rs = getMetaData().getSchemas();
            ArrayList<String> schemaList = New.arrayList();
            while (rs.next()) {
                schemaList.add(rs.getString("TABLE_SCHEM"));
            }
            JdbcUtils.closeSilently(rs);

            String jaquTables = DbVersion.class.getAnnotation(JQTable.class)
                    .name();

            List<TableInspector> tables = New.arrayList();
            if (schemaList.size() == 0) {
                schemaList.add(null);
            }
            for (String s : schemaList) {
                rs = getMetaData().getTables(null, s, null,
                        new String[] { "TABLE" });
                while (rs.next()) {
                    String t = rs.getString("TABLE_NAME");
                    if (!t.equalsIgnoreCase(jaquTables)) {
                        tables.add(new TableInspector(s, t, getMetaData()
                                .storesUpperCaseIdentifiers(), dateTimeClass));
                    }
                }
            }

            if (StringUtils.isNullOrEmpty(schema)
                    && StringUtils.isNullOrEmpty(table)) {
                // all schemas and tables
                return tables;
            }
            // schema subset OR table subset OR exact match
            List<TableInspector> matches = New.arrayList();
            for (TableInspector t : tables) {
                if (t.matches(schema, table)) {
                    matches.add(t);
                }
            }
            if (matches.size() == 0) {
                throw new RuntimeException(MessageFormat.format(
                        "Failed to find schema={0} table={1}",
                        schema == null ? "" : schema, table == null ? ""
                                : table));
            }
            return matches;
        } finally {
            JdbcUtils.closeSilently(rs);
        }
    }

}
