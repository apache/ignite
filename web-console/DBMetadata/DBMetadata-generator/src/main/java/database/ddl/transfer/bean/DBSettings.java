package database.ddl.transfer.bean;

import database.ddl.transfer.consts.DataBaseType;

/**
 * 数据库配置参数实体类
 * 
 * @author gs
 *
 */
public class DBSettings {
	
	/**
	 * 数据库类型
	 */
	private DataBaseType dataBaseType;

	/**
	 * 数据库驱动
	 */
	private String driverClass;
	
	/**
	 * 数据库连接ip地址
	 */
	private String ipAddress;
	
	/**
	 * 数据库连接端口
	 */
	private String port;

	/**
	 * 数据库名
	 */
	private String dataBaseName;

	/**
	 * 数据库用户名
	 */
	private String userName;

	/**
	 * 数据库密码
	 */
	private String userPassword;
	

	public DataBaseType getDataBaseType() {
		return dataBaseType;
	}

	public void setDataBaseType(DataBaseType dataBaseType) {
		this.dataBaseType = dataBaseType;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getDataBaseName() {
		return dataBaseName;
	}

	public void setDataBaseName(String dataBaseName) {
		this.dataBaseName = dataBaseName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserPassword() {
		return userPassword;
	}

	public void setUserPassword(String userPassword) {
		this.userPassword = userPassword;
	}


}
