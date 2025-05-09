package org.apache.ignite.console.dto;

import java.util.Properties;
import java.util.UUID;


import lombok.Data;

/**
 * add@byron 保存当前的关系数据库连接信息
 */
@Data
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
}
