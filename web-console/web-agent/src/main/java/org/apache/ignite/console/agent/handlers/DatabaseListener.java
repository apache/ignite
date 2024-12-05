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

import org.apache.ignite.console.agent.db.DataSourceManager;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.db.DBInfo;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.util.StringUtil;


/**
 * API to extract database metadata.
 */

public class DatabaseListener {

	/** Index of alive node URI. jndiName->DBInfo*/
	final public Map<String, DBInfo> clusters = new ConcurrentHashMap<>();
	final public Map<String, Integer> deactivedCluster = new ConcurrentHashMap<>();
	
	public boolean deactivedCluster(String id) {
		Integer count = deactivedCluster.compute(id,(k,v)->{ return v==null? 1: ++v;});
		if(count>1) {			
			return true;
		}
		return false;
	}
	
	/**
	 * 执行完以后，jndiName不为空
	 * @param args
	 * @param conn
	 * @return
	 * @throws IllegalArgumentException
	 */
	public DBInfo addDB(Map<String, Object> args,Connection conn) throws IllegalArgumentException {
		DBInfo dbInfo = new DBInfo();
		dbInfo.buildWith(args);

		String url = dbInfo.jdbcUrl;

		for (DBInfo dbInfo0 : clusters.values()) {
			if (dbInfo0.jdbcUrl.equalsIgnoreCase(url)) {
				return dbInfo;
			}
		}

		if(dbInfo.getId()==null) {
			dbInfo.setId(UUID.randomUUID());
		}
		
		if(!StringUtil.isBlank(dbInfo.jndiName)) {
			
			DataSourceManager.bindDataSource(dbInfo.jndiName, dbInfo);
			
			clusters.put(dbInfo.getId().toString(), dbInfo);
		}
		else {
			try {
				String dbProductName = conn.getMetaData().getDatabaseProductName();
				
				String catalog = conn.getCatalog();
				
				
				if(StringUtil.isEmpty(catalog)) {
					catalog = conn.getSchema();
				}
				
				if(StringUtil.isEmpty(catalog)) {
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
				
				dbInfo.jndiName= String.format("ds%s_%s",dbProductName,catalog.replaceAll("_", "").replace('-','_'));					
				DataSourceManager.bindDataSource(dbInfo.jndiName, dbInfo);
				
				//- 保存datasource
				//-DataSourceManager.createDataSource(dbInfo.getId().toString(), dbInfo);
				
				clusters.put(dbInfo.getId().toString(), dbInfo);
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		return dbInfo;
	}

	public DBInfo findDB(Map<String, Object> args) throws Exception {
		DBInfo dbInfoTarget = new DBInfo();
		dbInfoTarget.buildWith(args);

		for (DBInfo dbInfo : clusters.values()) {
			if (dbInfoTarget.jdbcUrl!=null && dbInfo.jdbcUrl.equalsIgnoreCase(dbInfoTarget.jdbcUrl)) {
				return dbInfo;
			}
			if (dbInfoTarget.jndiName!=null && dbInfo.jndiName!=null && dbInfo.jndiName.equalsIgnoreCase(dbInfoTarget.jndiName)) {
				return dbInfo;
			}
		}
		return null;
	}

	public boolean isDBCluster(String clusterId) {
		return clusters.containsKey(clusterId);
	}
	
	public DBInfo getDBClusterInfo(String clusterId) {
		deactivedCluster.remove(clusterId);
		return clusters.get(clusterId);
	}
	
	public void clear() {
		clusters.clear();
	}	
	
	public Connection getConnection(DBInfo dbInfo) {
		try {
			Connection conn = null;
			DataSource ds = DataSourceManager.getDataSource(dbInfo.jndiName);
        	if(ds!=null) {
        		conn = ds.getConnection();
        	}        	
        	return conn;
		} catch (SQLException e) {
			return null;
		}
	}
}
