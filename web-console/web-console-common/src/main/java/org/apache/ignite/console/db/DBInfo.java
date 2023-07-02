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
	
	private UUID id; // db唯一ID	
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
	
	public transient TopologySnapshot top;

	public DBInfo() {
		
	}
	
	public DBInfo(UUID id) {
		this.id = id;
	}

	public DBInfo(String jndiName, String currentDriverCls, String currentJdbcUrl) {
		super();
		this.jndiName = jndiName;
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
	}

	public DBInfo(String jndiName, String currentDriverCls, String currentJdbcUrl, Properties currentJdbcInfo) {
		super();
		this.jndiName = jndiName;
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
		this.jdbcProp = currentJdbcInfo;
	}
	
	
	public boolean isTablesOnly() {
		return tablesOnly;
	}

	public void setTablesOnly(boolean tablesOnly) {
		this.tablesOnly = tablesOnly;
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

	public String getDriverJar() {
		return driverJar;
	}

	public void setDriverJar(String driverJar) {
		this.driverJar = driverJar;
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
	

	public DBInfo buildWith(Map<String, Object> args) throws IllegalArgumentException {
		if (args.containsKey("jdbcDriverJar"))
			driverJar = args.get("jdbcDriverJar").toString();
		
		driverCls = (String)args.get("jdbcDriverClass");
		if(driverCls==null) {
			driverCls = (String) args.get("driverCls");
		}
		
		if (driverCls==null)
			throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

		
		if (!args.containsKey("jdbcUrl"))
			throw new IllegalArgumentException("Missing url in arguments: " + args);

		jdbcUrl = args.get("jdbcUrl").toString();
		
		if (args.containsKey("jndiName"))	
			jndiName = args.get("jndiName").toString();
		
		if (args.containsKey("id")) {
			try {
				id = UUID.fromString(args.get("id").toString());
			}
			catch(IllegalArgumentException e) {
				
			}
		}
		
		if (args.containsKey("schema"))	
			schemaName = args.get("schema").toString();
		
		if (args.containsKey("userName"))	
			userName = args.get("userName").toString();
		
		if (args.containsKey("password"))	
			password = args.get("password").toString();

		
		jdbcProp = new Properties();
		jdbcProp.put("user", userName);
		jdbcProp.put("password", password);
		if (args.get("info")!=null) {			
			jdbcProp.putAll((Map) args.get("info"));
		}
		if(args.get("jdbcProp")!=null) {
			jdbcProp.putAll((Map) args.get("jdbcProp"));
		}		
		
		if (args.containsKey("tablesOnly"))			
			tablesOnly = Boolean.valueOf(args.get("tablesOnly").toString());		
		
		return this;
	}

}
