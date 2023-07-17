package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.IgniteAdmin;
import de.kp.works.ignite.IgniteConf;
import de.kp.works.ignite.IgniteConstants;
import de.kp.works.ignite.ValueType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class IgniteGraphUtils {

    private static final Map<String, IgniteConnection> connections = new ConcurrentHashMap<>();

    public static IgniteConnection getConnection(IgniteGraphConfiguration config) {

        IgniteConnection conn;

        String namespace = config.getGraphNamespace();

        /* Check whether connection already exists */
        conn = connections.get(namespace);
        if (conn != null) return conn;
        
        String igniteCfg = config.getString("gremlin.graph.ignite.cfg");
        if(igniteCfg!=null && !igniteCfg.isEmpty()) {
        	IgniteConf.file = igniteCfg;
        }  

        conn = new IgniteConnection(namespace);

        connections.put(config.getGraphNamespace(), conn);
        return conn;
    }

    public static String getTableName(IgniteGraphConfiguration config, String name) {
        String ns = config.getGraphNamespace();
        return ns + "_" + name;
    }

    /**
     * This method is used in the initialization phase
     * of the [IgniteGraph] and creates the respective
     * Ignite caches.
      */
    public static void createTables(IgniteGraphConfiguration config, IgniteAdmin admin) {
        createTable(admin, getTableName(config, IgniteConstants.EDGES));
        createTable(admin, getTableName(config, IgniteConstants.VERTICES));
    }

    private static void createTable(IgniteAdmin admin, String name) {
        if (admin.tableExists(name)) return;
        admin.createTable(name);
    }

    public static void dropTables(IgniteGraphConfiguration config, IgniteConnection conn) throws Exception {
        IgniteAdmin admin = conn.getAdmin();
        dropTables(config,admin);
    }

    private static void dropTables(IgniteGraphConfiguration config, IgniteAdmin admin) throws Exception {
        dropTable(admin, getTableName(config, IgniteConstants.EDGES));
        dropTable(admin, getTableName(config, IgniteConstants.VERTICES));
    }

    private static void dropTable(IgniteAdmin admin, String name) throws Exception {
        if (!admin.tableExists(name)) return;
        admin.dropTable(name);
    }

    public static Object generateIdIfNeeded(Object id) {
        if (id == null) {
            id = UUID.randomUUID().toString();
        } else if (id instanceof Long) {
            // noop
        } else if (id instanceof Number) {
            id = ((Number) id).longValue();
        }
        return id;
    }

    public static Map<String, Object> propertiesToMap(Object... keyValues) {
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object value = keyValues[i + 1];
            if (value == null) continue;
            ElementHelper.validateProperty(keyStr, value);
            // add@byron
            if(IgniteGraph.stringedPropertyType) {
            	if(value.getClass().isArray() && value instanceof Object[]) {
            		props.put(keyStr, value);
            	}
            	else if(value instanceof Collection) {
            		props.put(keyStr, value);
            	}
            	else {
            		props.put(keyStr, value.toString());
            	}
            }
            else {
            	props.put(keyStr, value);
            }
        }
        return props;
    }

    public static Map<String, ValueType> propertyKeysAndTypesToMap(Object... keyTypes) {
        Map<String, ValueType> props = new HashMap<>();
        for (int i = 0; i < keyTypes.length; i = i + 2) {
            Object key = keyTypes[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object type = keyTypes[i + 1];
            ValueType valueType;
            if (type instanceof ValueType) {
                valueType = (ValueType) type;
            } else {
                valueType = ValueType.valueOf(type.toString().toUpperCase());
            }
            props.put(keyStr, valueType);
        }
        return props;
    }

}
