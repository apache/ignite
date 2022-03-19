package org.apache.ignite.console.db;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.ignite.console.websocket.TopologySnapshot;

import lombok.Data;

/**
 * add@byron 保存当前的关系数据库连接信息
 */

public class DBInfo {
	public String clusterId; // db连接，部署的clusterID
	private UUID id; // 用户ID	
	private UUID accId; // 用户ID	
	public String driverJar; // jar路径
	public String driverCls;
	public String jdbcUrl;
	public String jndiName;
	public String schemaName; // 默认的模式名称
	private String userName;
	private String password;
	public Properties jdbcProp;
	boolean tablesOnly = true;
	
	public boolean isTablesOnly() {
		return tablesOnly;
	}

	public void setTablesOnly(boolean tablesOnly) {
		this.tablesOnly = tablesOnly;
	}

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public UUID getAccId() {
		return accId;
	}

	public void setAccId(UUID accId) {
		this.accId = accId;
	}

	public String getDriverCls() {
		return driverCls;
	}

	public void setDriverCls(String driverCls) {
		this.driverCls = driverCls;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getJndiName() {
		return jndiName;
	}

	public void setJndiName(String jndiName) {
		this.jndiName = jndiName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Properties getJdbcProp() {
		return jdbcProp;
	}

	public void setJdbcProp(Properties jdbcProp) {
		this.jdbcProp = jdbcProp;
	}

	public transient TopologySnapshot top;
	
	public DBInfo() {
		
	}

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
	

	public DBInfo buildWith(Map<String, Object> args) throws IllegalArgumentException {
		if (args.containsKey("jdbcDriverJar"))
			driverJar = args.get("jdbcDriverJar").toString();

		if (!args.containsKey("jdbcDriverClass"))
			throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

		driverCls = args.get("jdbcDriverClass").toString();

		if (!args.containsKey("jdbcUrl"))
			throw new IllegalArgumentException("Missing url in arguments: " + args);

		jdbcUrl = args.get("jdbcUrl").toString();
		
		if (args.containsKey("jndiName"))	
			jndiName = args.get("jndiName").toString();

		if (args.containsKey("info")) {
			Properties info = new Properties();
			info.putAll((Map) args.get("info"));
		}
		
		if (args.containsKey("schema"))	
			schemaName = args.get("schema").toString();
		
		if (args.containsKey("userName"))	
			userName = args.get("userName").toString();
		
		if (args.containsKey("password"))	
			password = args.get("password").toString();

		if (args.containsKey("tablesOnly"))			
			tablesOnly = Boolean.valueOf(args.get("tablesOnly").toString());		
		
		return this;
	}

	public String getDriverJar() {
		return driverJar;
	}

	public void setDriverJar(String driverJar) {
		this.driverJar = driverJar;
	}

}
