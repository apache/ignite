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
	private String driverCls;
	private String jdbcUrl;	
	private String dbName; // 数据库名称
	private String schemaName; // 默认的模式名称
	private String userName;
	private String password;
	private Properties jdbcProp;
	
	private DBInfoDto() {
		
	}
	
	public DBInfoDto(String currentDriverCls, String currentJdbcUrl) {
		super(UUID.randomUUID());		
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
	}

	public DBInfoDto(String dbId, String currentDriverCls, String currentJdbcUrl) {
		super(dbId);		
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
	}

	public DBInfoDto(String dbId, String currentDriverCls, String currentJdbcUrl, Properties currentJdbcInfo) {
		super(dbId);	
		this.driverCls = currentDriverCls;
		this.jdbcUrl = currentJdbcUrl;
		this.jdbcProp = currentJdbcInfo;
	}
}
