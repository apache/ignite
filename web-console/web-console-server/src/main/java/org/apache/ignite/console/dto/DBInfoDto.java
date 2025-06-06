package org.apache.ignite.console.dto;

import java.util.Properties;
import java.util.UUID;


/**
 * add@byron 保存当前的关系数据库连接信息
 */

public class DBInfoDto extends AbstractDto {
	private UUID accId; // 用户ID
	private String db; // 数据库类型
	private String driverCls;
	private String jdbcUrl;	
	private String jndiName; // 数据库唯一标识名称
	private String schemaName; // 默认的模式名称
	private String userName;
	private String password;
	private Properties jdbcProp;
	
	public DBInfoDto() {
		
	}
	
	public DBInfoDto(String dbId) {
		super(dbId);
	}
	
	public DBInfoDto(String currentDriverCls, String currentJdbcUrl) {
		super(UUID.randomUUID());		
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
	}
	
	public DBInfoDto(String jndiName,String currentDriverCls, String currentJdbcUrl) {		
		this.jndiName = jndiName;
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
	}

	public DBInfoDto(String jndiName,String currentDriverCls, String currentJdbcUrl, Properties currentJdbcInfo) {		
		this.jndiName = jndiName;
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
		this.jdbcProp = currentJdbcInfo;
	}

	public UUID getAccId() {
		return accId;
	}

	public void setAccId(UUID accId) {
		this.accId = accId;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
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
}
