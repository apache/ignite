package org.apache.ignite.console.db;

import java.util.Properties;

import org.apache.ignite.console.websocket.TopologySnapshot;

/**
 * add@byron 保存当前的关系数据库连接信息
 */
public class DBInfo {
	public final String clusterId;
	public String driverCls;
	public String jdbcUrl;
	public String jndiName;
	public Properties jdbcProp;
	public TopologySnapshot top;

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
