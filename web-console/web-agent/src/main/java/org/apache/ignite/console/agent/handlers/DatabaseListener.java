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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;


/**
 * API to extract database metadata.
 */

public class DatabaseListener {

	/** Index of alive node URI. */
	final public Map<String, DBInfo> clusters = U.newHashMap(2);

	/**
	 * add@byron 保存当前的关系数据库连接信息
	 */
	public static class DBInfo {
		public final String clusterId;
		public String driverCls;
		public String jdbcUrl;
		public Properties jdbcProp;

		public DBInfo(String clusterId, String currentDriverCls, String currentJdbcUrl) {
			super();
			this.clusterId = clusterId;
			this.driverCls = currentDriverCls;
			this.jdbcUrl = currentJdbcUrl;
		}

		public DBInfo(String clusterId, String currentDriverCls, String currentJdbcUrl, Properties currentJdbcInfo) {
			super();
			this.clusterId = clusterId;
			this.driverCls = currentDriverCls;
			this.jdbcUrl = currentJdbcUrl;
			this.jdbcProp = currentJdbcInfo;
		}
	}

	public DBInfo addDB(Map<String, Object> args) throws IllegalArgumentException {
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

		List<String> schemas = (List<String>) args.get("schemas");

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
}
