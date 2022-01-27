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

package org.apache.ignite.console.agent.handlers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.console.agent.db.DataSourceManager;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.db.DBInfo;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;


/**
 * API to extract database metadata.
 */

public class DatabaseListener {

	/** Index of alive node URI. */
	final public Map<String, DBInfo> clusters = new ConcurrentHashMap<>();
	final public DataSourceManager dataSourceManager = new DataSourceManager();
	

	public DBInfo addDB(Map<String, Object> args,Connection conn) throws IllegalArgumentException {
		String driverPath = null;

		String url = args.get("jdbcUrl").toString();

		for (DBInfo dbInfo : clusters.values()) {
			if (dbInfo.jdbcUrl.equalsIgnoreCase(url)) {
				return dbInfo;
			}
		}
		
		if (args.containsKey("jdbcDriverJar"))
			driverPath = args.get("jdbcDriverJar").toString();

		if (!args.containsKey("jdbcDriverClass"))
			throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

		String driverCls = args.get("jdbcDriverClass").toString();

		if (!args.containsKey("jdbcUrl"))
			throw new IllegalArgumentException("Missing url in arguments: " + args);

		if (!args.containsKey("info"))
			throw new IllegalArgumentException("Missing info in arguments: " + args);

		Properties info = new Properties();

		info.putAll((Map) args.get("info"));

		String clusterId = UUID.randomUUID().toString();
		DBInfo dbInfo = new DBInfo(clusterId, driverCls, url, info);
		
		clusters.put(clusterId, dbInfo);
		if(conn!=null) {
			try {
				String dbProductName = conn.getMetaData().getDatabaseProductName();
				
				String catalog = conn.getCatalog();
				
				
				if(StringUtils.isEmpty(catalog)) {
					catalog = conn.getSchema();
				}
				
				if(StringUtils.isEmpty(catalog)) {
					ResultSet result = conn.getMetaData().getCatalogs();
					if(result.next()){
						catalog = result.getString(1);
					}
				}
				
				if(catalog.equals("DREMIO")) {
					dbProductName = "Dremio";
					int pos = url.indexOf("schema=");
					if(pos>0) {
						int endPos = url.indexOf(';',pos);
						if(endPos<0) {
							endPos = url.length();
						}
						catalog = url.substring(pos+7,endPos); 
					}
				}
				
				dbInfo.jndiName= String.format("java:jdbc/ds%s_%s",dbProductName,catalog.replaceAll("_", "").replace('-','_'));
				
				dataSourceManager.bindDataSource(dbInfo.jndiName, dbInfo.jdbcUrl, info);
				
				
				dbInfo.jndiName= String.format("java:jdbc/ds%s_%s","Generic",catalog.replaceAll("_", "").replace('-','_'));
				
				dataSourceManager.bindDataSource(dbInfo.jndiName, dbInfo.jdbcUrl, info);
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return dbInfo;
	}

	public DBInfo findDB(Map<String, Object> args) throws Exception {
		String driverPath = null;

		if (args.containsKey("jdbcDriverJar"))
			driverPath = args.get("jdbcDriverJar").toString();

		if (!args.containsKey("jdbcDriverClass"))
			throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

		String driverCls = args.get("jdbcDriverClass").toString();

		if (!args.containsKey("jdbcUrl"))
			throw new IllegalArgumentException("Missing url in arguments: " + args);

		String url = args.get("jdbcUrl").toString();

		if (!args.containsKey("info"))
			throw new IllegalArgumentException("Missing info in arguments: " + args);

		
		if (!args.containsKey("schemas"))
			throw new IllegalArgumentException("Missing schemas in arguments: " + args);

		//List<String> schemas = (List<String>) args.get("schemas");

		if (!args.containsKey("tablesOnly"))
			throw new IllegalArgumentException("Missing tablesOnly in arguments: " + args);

		for (DBInfo dbInfo : clusters.values()) {
			if (dbInfo.jdbcUrl.equalsIgnoreCase(url)) {
				return dbInfo;
			}
		}
		return null;
	}

	public boolean isDBCluster(String clusterId) {
		return clusters.containsKey(clusterId);
	}
	
	public DBInfo getDBClusterInfo(String clusterId) {
		return clusters.get(clusterId);
	}
	
	public void clear() {
		clusters.clear();
	}	
	
	public DataSource getDataSource(String jdbcUrl) {
		try {
			return dataSourceManager.getDataSource(jdbcUrl);
		} catch (SQLException e) {
			return null;
		}
	}
}
