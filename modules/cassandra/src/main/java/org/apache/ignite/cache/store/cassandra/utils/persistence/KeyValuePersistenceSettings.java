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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.utils.common.SystemHelper;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Stores persistence settings for Ignite cache key and value
 */
public class KeyValuePersistenceSettings {
    private static final String KEYSPACE_ATTR = "keyspace";
    private static final String TABLE_ATTR = "table";
    private static final String LOAD_ON_STARTUP_ATTR = "loadOnStartup";
    private static final String TTL_ATTR = "ttl";
    private static final String PERSISTENCE_NODE = "persistence";
    private static final String KEYSPACE_OPTIONS_NODE = "keyspaceOptions";
    private static final String TABLE_OPTIONS_NODE = "tableOptions";
    private static final String KEY_PERSISTENCE_NODE = "keyPersistence";
    private static final String VALUE_PERSISTENCE_NODE = "valuePersistence";

    private int startupLoadingCount = 10000;
    private Integer ttl;
    private String keyspace;
    private String table;
    private String tableOptions;
    private String keyspaceOptions = "replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3} " +
        "and durable_writes = true";

    private KeyPersistenceSettings keyPersistenceSettings;
    private ValuePersistenceSettings valuePersistenceSettings;

    @SuppressWarnings("UnusedDeclaration")
    public KeyValuePersistenceSettings(String settings) {
        init(settings);
    }

    public KeyValuePersistenceSettings(Resource settingsResource) {
        init(loadSettings(settingsResource));
    }

    public int getStartupLoadingCount() {
        return startupLoadingCount;
    }

    public Integer getTTL() {
        return ttl;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getTableFullName()
    {
        return keyspace + "." + table;
    }

    public KeyPersistenceSettings getKeyPersistenceSettings() {
        return keyPersistenceSettings;
    }

    public ValuePersistenceSettings getValuePersistenceSettings() {
        return valuePersistenceSettings;
    }

    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getFields() {
        List<PojoField> fields = new LinkedList<>();

        for (PojoField field : keyPersistenceSettings.getFields())
            fields.add(field);

        for (PojoField field : valuePersistenceSettings.getFields())
            fields.add(field);

        return fields;
    }

    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getKeyFields() {
        return keyPersistenceSettings.getFields();
    }

    @SuppressWarnings("UnusedDeclaration")
    public List<PojoField> getValueFields() {
        return valuePersistenceSettings.getFields();
    }

    public String getKeyspaceDDLStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("create keyspace if not exists ").append(keyspace);

        if (keyspaceOptions != null) {
            if (!keyspaceOptions.trim().toLowerCase().startsWith("with"))
                builder.append(" with");

            builder.append(" ").append(keyspaceOptions);
        }

        String statement = builder.toString().trim();

        if (!statement.endsWith(";"))
            statement += ";";

        return statement;
    }

    public String getTableDDLStatement() {
        String columnsDDL = keyPersistenceSettings.getTableColumnsDDL() + ", " + valuePersistenceSettings.getTableColumnsDDL();

        String primaryKeyDDL = keyPersistenceSettings.getPrimaryKeyDDL();
        String clusteringDDL = keyPersistenceSettings.getClusteringDDL();

        String optionsDDL = tableOptions != null && !tableOptions.trim().isEmpty() ? tableOptions.trim() : "";

        if (clusteringDDL != null && !clusteringDDL.isEmpty())
            optionsDDL = optionsDDL.isEmpty() ? clusteringDDL : optionsDDL + " and " + clusteringDDL;

        if (!optionsDDL.trim().isEmpty())
            optionsDDL = optionsDDL.trim().toLowerCase().startsWith("with") ? optionsDDL.trim() : "with " + optionsDDL.trim();

        StringBuilder builder = new StringBuilder();
        builder.append("create table if not exists ").append(keyspace).append(".").append(table);
        builder.append(" (").append(columnsDDL).append(", ").append(primaryKeyDDL).append(")");

        if (!optionsDDL.isEmpty())
            builder.append(" ").append(optionsDDL);

        String tableDDL = builder.toString().trim();

        return tableDDL.endsWith(";") ? tableDDL : tableDDL + ";";
    }

    public List<String> getIndexDDLStatements() {
        List<String> indexDDLs = new LinkedList<>();

        List<PojoField> fields = valuePersistenceSettings.getFields();

        for (PojoField field : fields) {
            if (((PojoValueField)field).isIndexed())
                indexDDLs.add(((PojoValueField)field).getIndexDDL(keyspace, table));
        }

        return indexDDLs;
    }

    private String loadSettings(Resource resource) {
        StringBuilder settings = new StringBuilder();
        InputStream in;
        BufferedReader reader = null;

        try {
            in = resource.getInputStream();
        }
        catch (IOException e) {
            throw new IgniteException("Failed to get input stream for Cassandra persistence settings resource: " + resource, e);
        }

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
            throw new IgniteException("Failed to read input stream for Cassandra persistence settings resource: " + resource, e);
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (Throwable ignored) {
                }
            }

            if (in != null) {
                try {
                    in.close();
                }
                catch (Throwable ignored) {
                }
            }
        }

        return settings.toString();
    }

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
            throw new IllegalArgumentException("Incorrect persistence settings specified. Root XML element " +
                "should be 'persistence'");
        }

        if (!root.hasAttribute(KEYSPACE_ATTR)) {
            throw new IllegalArgumentException("Incorrect persistence settings '" + KEYSPACE_ATTR + "' attribute " +
                "should be specified");
        }

        if (!root.hasAttribute(TABLE_ATTR)) {
            throw new IllegalArgumentException("Incorrect persistence settings '" + TABLE_ATTR + "' attribute " +
                "should be specified");
        }

        keyspace = root.getAttribute(KEYSPACE_ATTR).trim();
        table = root.getAttribute(TABLE_ATTR).trim();

        if (root.hasAttribute(LOAD_ON_STARTUP_ATTR)) {
            String val = root.getAttribute(LOAD_ON_STARTUP_ATTR).trim();

            try {
                startupLoadingCount = Integer.parseInt(val);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Incorrect value '" + val + "' specified for '" + LOAD_ON_STARTUP_ATTR + "' attribute");
            }
        }

        if (root.hasAttribute(TTL_ATTR)) {
            String val = root.getAttribute(TTL_ATTR).trim();

            try {
                ttl = Integer.parseInt(val);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Incorrect value '" + val + "' specified for '" + TTL_ATTR + "' attribute");
            }
        }

        if (!root.hasChildNodes()) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no key and value persistence settings specified");
        }

        NodeList children = root.getChildNodes();
        int count = children.getLength();

        for (int i = 0; i < count; i++) {
            Node node = children.item(i);

            if (node.getNodeType() != Node.ELEMENT_NODE)
                continue;

            Element el = (Element)node;
            String nodeName = el.getNodeName();

            if (nodeName.equals(TABLE_OPTIONS_NODE)) {
                tableOptions = el.getTextContent();
                tableOptions = tableOptions.replace("\n", " ").replace("\r", "");
            }
            else if (nodeName.equals(KEYSPACE_OPTIONS_NODE)) {
                keyspaceOptions = el.getTextContent();
                keyspaceOptions = keyspaceOptions.replace("\n", " ").replace("\r", "");
            }
            else if (nodeName.equals(KEY_PERSISTENCE_NODE))
                keyPersistenceSettings = new KeyPersistenceSettings(el);
            else if (nodeName.equals(VALUE_PERSISTENCE_NODE))
                valuePersistenceSettings = new ValuePersistenceSettings(el);
        }

        if (keyPersistenceSettings == null) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no key persistence settings specified");
        }

        if (valuePersistenceSettings == null) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no value persistence settings specified");
        }

        List<PojoField> keyFields = keyPersistenceSettings.getFields();
        List<PojoField> valueFields = valuePersistenceSettings.getFields();

        if (PersistenceStrategy.POJO.equals(keyPersistenceSettings.getStrategy()) &&
            (keyFields == null || keyFields.isEmpty())) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no key fields found");
        }

        if (PersistenceStrategy.POJO.equals(valuePersistenceSettings.getStrategy()) &&
            (valueFields == null || valueFields.isEmpty())) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                " there are no value fields found");
        }

        if (keyFields == null || keyFields.isEmpty() || valueFields == null || valueFields.isEmpty())
            return;

        for (PojoField keyField : keyFields) {
            for (PojoField valField : valueFields) {
                if (keyField.getColumn().equals(valField.getColumn())) {
                    throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification," +
                        " key column '" + keyField.getColumn() + "' also specified as a value column");
                }
            }
        }
    }
}
