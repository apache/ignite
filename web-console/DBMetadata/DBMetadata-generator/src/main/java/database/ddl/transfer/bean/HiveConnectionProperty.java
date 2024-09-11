package database.ddl.transfer.bean;

/**
 * @ClassName HiveConnectionProperty
 * @Description
 * @Author luoyuntian
 * @Date 2020-01-15 10:02
 * @Version
 **/
public class HiveConnectionProperty {
	
	/**
	 * 连接名
	 */
	private String url;
	
	/**
	 * 用户名
	 */
	private String userName;
	
	/**
	 * 密码
	 */
	private String password;
	
	/**
	 * 驱动
	 */
	private String driver;

	public HiveConnectionProperty(String url, String userName, String password, String driver) {
		this.url = url;
		this.userName = userName;
		this.password = password;
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
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

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}
}
