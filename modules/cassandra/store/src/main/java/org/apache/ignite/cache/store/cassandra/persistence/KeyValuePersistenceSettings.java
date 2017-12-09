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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Collections;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.CassandraHelper;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.cache.store.cassandra.handler.TypeHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Stores persistence settings for Ignite cache key and value
 */
public class KeyValuePersistenceSettings implements Serializable {
    /**
     * Default Cassandra keyspace options which should be used to create new keyspace.
     * <ul>
     * <li> <b>SimpleStrategy</b> for replication work well for single data center Cassandra cluster.<br/>
     *      If your Cassandra cluster deployed across multiple data centers it's better to use <b>NetworkTopologyStrategy</b>.
     * </li>
     * <li> Three replicas will be created for each data block. </li>
     * <li> Setting DURABLE_WRITES to true specifies that all data should be written to commit log. </li>
     * </ul>
     */
    private static final String DFLT_KEYSPACE_OPTIONS = "replication = {'class' : 'SimpleStrategy', " +
            "'replication_factor' : 3} and durable_writes = true";

    /** Xml attribute specifying Cassandra keyspace to use. */
    private static final String KEYSPACE_ATTR = "keyspace";

    /** Xml attribute specifying Cassandra table to use. */
    private static final String TABLE_ATTR = "table";

    /** Xml attribute specifying ttl (time to leave) for rows inserted in Cassandra. */
    private static final String TTL_ATTR = "ttl";

    /** Root xml element containing persistence settings specification. */
    private static final String PERSISTENCE_NODE = "persistence";

    /** Xml element specifying Cassandra keyspace options. */
    private static final String KEYSPACE_OPTIONS_NODE = "keyspaceOptions";

    /** Xml element specifying Cassandra table options. */
    private static final String TABLE_OPTIONS_NODE = "tableOptions";

    /** Xml element specifying Ignite cache key persistence settings. */
    private static final String KEY_PERSISTENCE_NODE = "keyPersistence";

    /** Xml element specifying Ignite cache value persistence settings. */
    private static final String VALUE_PERSISTENCE_NODE = "valuePersistence";

    /** TTL (time to leave) for rows inserted into Cassandra table {@link <a href="https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_expire_c.html">Expiring data</a>}. */
    private Integer ttl;

    /** Cassandra keyspace (analog of tablespace in relational databases). */
    private String keyspace;

    /** Cassandra table. */
    private String tbl;

    /** Cassandra table creation options {@link <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html">CREATE TABLE</a>}. */
    private String tblOptions;

    /** Cassandra keyspace creation options {@link <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html">CREATE KEYSPACE</a>}. */
    private String keyspaceOptions = DFLT_KEYSPACE_OPTIONS;

    /** Persistence settings for Ignite cache keys. */
    private KeyPersistenceSettings keyPersistenceSettings;

    /** Persistence settings for Ignite cache values. */
    private ValuePersistenceSettings valPersistenceSettings;

    /** List of Cassandra table columns */
    private List<String> tableColumns;

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settings string containing xml with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public KeyValuePersistenceSettings(String settings) {
        init(settings);
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settingsFile xml file with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public KeyValuePersistenceSettings(File settingsFile) {
        InputStream in;

        try {
            in = new FileInputStream(settingsFile);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to get input stream for Cassandra persistence settings file: " +
                    settingsFile.getAbsolutePath(), e);
        }

        init(loadSettings(in));
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settingsRsrc resource containing xml with persistence settings for Ignite cache key/value
     */
    public KeyValuePersistenceSettings(Resource settingsRsrc) {
        InputStream in;

        try {
            in = settingsRsrc.getInputStream();
        }
        catch (IOException e) {
            throw new IgniteException("Failed to get input stream for Cassandra persistence settings resource: " + settingsRsrc, e);
        }

        init(loadSettings(in));
    }

    /**
     * Returns ttl to use for while inserting new rows into Cassandra table.
     *
     * @return ttl
     */
    public Integer getTTL() {
        return ttl;
    }

    /**
     * Returns Cassandra keyspace to use.
     *
     * @return keyspace.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Returns Cassandra table to use.
     *
     * @return table.
     */
    public String getTable() {
        return tbl;
    }

    /**
     * Returns persistence settings for Ignite cache keys.
     *
     * @return keys persistence settings.
     */
    public KeyPersistenceSettings getKeyPersistenceSettings() {
        return keyPersistenceSettings;
    }

    /**
     * Returns persistence settings for Ignite cache values.
     *
     * @return values persistence settings.
     */
    public ValuePersistenceSettings getValuePersistenceSettings() {
        return valPersistenceSettings;
    }

    /**
     * Returns list of POJO fields to be mapped to Cassandra table columns.
     *
     * @return POJO fields list.
     */
    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getFields() {
        List<PojoField> fields = new LinkedList<>();

        for (PojoField field : keyPersistenceSettings.getFields())
            fields.add(field);

        for (PojoField field : valPersistenceSettings.getFields())
            fields.add(field);

        return fields;
    }

    /**
     * Returns list of Ignite cache key POJO fields to be mapped to Cassandra table columns.
     *
     * @return POJO fields list.
     */
    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getKeyFields() {
        return keyPersistenceSettings.getFields();
    }

    /**
     * Returns list of Ignite cache value POJO fields to be mapped to Cassandra table columns.
     *
     * @return POJO fields list.
     */
    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getValueFields() {
        return valPersistenceSettings.getFields();
    }

    /**
     * Returns DDL statement to create Cassandra keyspace.
     *
     * @return Keyspace DDL statement.
     */
    public String getKeyspaceDDLStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("create keyspace if not exists \"").append(keyspace).append("\"");

        if (keyspaceOptions != null) {
            if (!keyspaceOptions.trim().toLowerCase().startsWith("with"))
                builder.append("\nwith");

            builder.append(" ").append(keyspaceOptions);
        }

        String statement = builder.toString().trim().replaceAll(" +", " ");

        return statement.endsWith(";") ? statement : statement + ";";
    }

    /**
     * Returns column names for Cassandra table.
     *
     * @return Column names.
     */
    public List<String> getTableColumns() {
        return tableColumns;
    }

    /**
     * Returns DDL statement to create Cassandra table.
     *
     * @param table Table name.
     * @return Table DDL statement.
     */
    public String getTableDDLStatement(String table) {
        if (table == null || table.trim().isEmpty())
            throw new IllegalArgumentException("Table name should be specified");

        String keyColumnsDDL = keyPersistenceSettings.getTableColumnsDDL();
        String valColumnsDDL = valPersistenceSettings.getTableColumnsDDL(new HashSet<>(keyPersistenceSettings.getTableColumns()));

        String colsDDL = keyColumnsDDL;

        if (valColumnsDDL != null && !valColumnsDDL.trim().isEmpty())
            colsDDL += ",\n" + valColumnsDDL;

        String primaryKeyDDL = keyPersistenceSettings.getPrimaryKeyDDL();

        String clusteringDDL = keyPersistenceSettings.getClusteringDDL();

        String optionsDDL = tblOptions != null && !tblOptions.trim().isEmpty() ? tblOptions.trim() : "";

        if (clusteringDDL != null && !clusteringDDL.isEmpty())
            optionsDDL = optionsDDL.isEmpty() ? clusteringDDL : optionsDDL + " and " + clusteringDDL;

        if (!optionsDDL.trim().isEmpty())
            optionsDDL = optionsDDL.trim().toLowerCase().startsWith("with") ? optionsDDL.trim() : "with " + optionsDDL.trim();

        StringBuilder builder = new StringBuilder();

        builder.append("create table if not exists \"").append(keyspace).append("\".\"").append(table).append("\"");
        builder.append("\n(\n").append(colsDDL).append(",\n").append(primaryKeyDDL).append("\n)");

        if (!optionsDDL.isEmpty())
            builder.append(" \n").append(optionsDDL);

        String tblDDL = builder.toString().trim().replaceAll(" +", " ");

        return tblDDL.endsWith(";") ? tblDDL : tblDDL + ";";
    }

    /**
     * Returns DDL statements to create Cassandra table secondary indexes.
     *
     * @param table Table name.
     * @return DDL statements to create secondary indexes.
     */
    public List<String> getIndexDDLStatements(String table) {
        List<String> idxDDLs = new LinkedList<>();

        Set<String> keyColumns = new HashSet<>(keyPersistenceSettings.getTableColumns());
        List<PojoField> fields = valPersistenceSettings.getFields();

        for (PojoField field : fields) {
            if (!keyColumns.contains(field.getColumn()) && ((PojoValueField)field).isIndexed())
                idxDDLs.add(((PojoValueField)field).getIndexDDL(keyspace, table));
        }

        return idxDDLs;
    }

    /**
     * Loads Ignite cache persistence settings from resource.
     *
     * @param in Input stream.
     * @return String containing xml with Ignite cache persistence settings.
     */
    private String loadSettings(InputStream in) {
        StringBuilder settings = new StringBuilder();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(in));

            String line = reader.readLine();

            while (line != null) {
                if (settings.length() != 0)
                    settings.append(SystemHelper.LINE_SEPARATOR);

                settings.append(line);

                line = reader.readLine();
            }
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to read input stream for Cassandra persistence settings", e);
        }
        finally {
            U.closeQuiet(reader);
            U.closeQuiet(in);
        }

        return settings.toString();
    }

    /**
     * @param elem Element with data.
     * @param attr Attribute name.
     * @return Numeric value for specified attribute.
     */
    private int extractIntAttribute(Element elem, String attr) {
        String val = elem.getAttribute(attr).trim();

        try {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Incorrect value '" + val + "' specified for '" + attr + "' attribute");
        }
    }

    /**
     * Initializes persistence settings from XML string.
     *
     * @param settings XML string containing Ignite cache persistence settings configuration.
     */
    @SuppressWarnings("IfCanBeSwitch")
    private void init(String settings) {
        Document doc;

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(settings)));
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("Failed to parse persistence settings:" +
                SystemHelper.LINE_SEPARATOR + settings, e);
        }

        Element root = doc.getDocumentElement();

        if (!PERSISTENCE_NODE.equals(root.getNodeName())) {
            throw new IllegalArgumentException("Incorrect persistence settings specified. " +
                "Root XML element should be 'persistence'");
        }

        if (!root.hasAttribute(KEYSPACE_ATTR)) {
            throw new IllegalArgumentException("Incorrect persistence settings '" + KEYSPACE_ATTR +
                "' attribute should be specified");
        }

        keyspace = root.getAttribute(KEYSPACE_ATTR).trim();
        tbl = root.hasAttribute(TABLE_ATTR) ? root.getAttribute(TABLE_ATTR).trim() : null;

        if (root.hasAttribute(TTL_ATTR))
            ttl = extractIntAttribute(root, TTL_ATTR);

        if (!root.hasChildNodes()) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                "there are no key and value persistence settings specified");
        }

        NodeList children = root.getChildNodes();
        int cnt = children.getLength();

        for (int i = 0; i < cnt; i++) {
            Node node = children.item(i);

            if (node.getNodeType() != Node.ELEMENT_NODE)
                continue;

            Element el = (Element)node;
            String nodeName = el.getNodeName();

            if (nodeName.equals(TABLE_OPTIONS_NODE)) {
                tblOptions = el.getTextContent();
                tblOptions = tblOptions.replace("\n", " ").replace("\r", "").replace("\t", " ");
            }
            else if (nodeName.equals(KEYSPACE_OPTIONS_NODE)) {
                keyspaceOptions = el.getTextContent();
                keyspaceOptions = keyspaceOptions.replace("\n", " ").replace("\r", "").replace("\t", " ");
            }
            else if (nodeName.equals(KEY_PERSISTENCE_NODE))
                keyPersistenceSettings = new KeyPersistenceSettings(el);
            else if (nodeName.equals(VALUE_PERSISTENCE_NODE))
                valPersistenceSettings = new ValuePersistenceSettings(el);
        }

        if (keyPersistenceSettings == null) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                "there are no key persistence settings specified");
        }

        if (valPersistenceSettings == null) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                "there are no value persistence settings specified");
        }

        List<PojoField> keyFields = keyPersistenceSettings.getFields();
        List<PojoField> valFields = valPersistenceSettings.getFields();

        if (PersistenceStrategy.POJO == keyPersistenceSettings.getStrategy() &&
            (keyFields == null || keyFields.isEmpty())) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                "there are no key fields found");
        }

        if (PersistenceStrategy.POJO == valPersistenceSettings.getStrategy() &&
            (valFields == null || valFields.isEmpty())) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                "there are no value fields found");
        }

        // Validating aliases compatibility - fields having different names, but mapped to the same Cassandra table column.
        if (valFields != null && !valFields.isEmpty()) {
            String keyColumn = keyPersistenceSettings.getColumn();
            TypeHandler typeHandler = keyPersistenceSettings.getTypeHandler();

            if (keyColumn != null && !keyColumn.isEmpty()) {
                for (PojoField valField : valFields) {
                    if (keyColumn.equals(valField.getColumn()) &&
                            !CassandraHelper.isCassandraCompatibleTypes(typeHandler, valField.getTypeHandler())) {
                        throw new IllegalArgumentException("Value field '" + valField.getName() + "' shares the same " +
                                "Cassandra table column '" + keyColumn + "' with key, but their Java classes are " +
                                "different. Fields sharing the same column should have the same Java class as their " +
                                "type or should be mapped to the same Cassandra primitive type or should have the same instance of type handlers.");
                    }
                }
            }

            if (keyFields != null && !keyFields.isEmpty()) {
                for (PojoField keyField : keyFields) {
                    for (PojoField valField : valFields) {
                        if (keyField.getColumn().equals(valField.getColumn()) &&
                                !CassandraHelper.isCassandraCompatibleTypes(keyField.getTypeHandler(), valField.getTypeHandler())) {
                            throw new IllegalArgumentException("Value field '" + valField.getName() + "' shares the same " +
                                    "Cassandra table column '" + keyColumn + "' with key field '" + keyField.getName() + "', " +
                                    "but their Java classes are different. Fields sharing the same column should have " +
                                    "the same Java class as their type or should be mapped to the same Cassandra " +
                                    "primitive type or should have the same instance of type handlers.");
                        }
                    }
                }
            }
        }

        tableColumns = new LinkedList<>();

        for (String column : keyPersistenceSettings.getTableColumns()) {
            if (!tableColumns.contains(column))
                tableColumns.add(column);
        }

        for (String column : valPersistenceSettings.getTableColumns()) {
            if (!tableColumns.contains(column))
                tableColumns.add(column);
        }

        tableColumns = Collections.unmodifiableList(tableColumns);
    }
}
