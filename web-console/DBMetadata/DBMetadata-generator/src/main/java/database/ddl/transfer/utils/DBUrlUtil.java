package database.ddl.transfer.utils;

import database.ddl.transfer.bean.DBSettings;

/**
 * 数据库URL处理工具类
 * 
 * @author gs
 *
 */
public class DBUrlUtil {

	/**
	 * 生成数据库url
	 * 
	 * @param settings 数据库配置
	 * @return
	 */
	public static String generateDataBaseUrl(DBSettings settings) {
		StringBuilder sb = new StringBuilder("");
		switch (settings.getDataBaseType()) {
			case MYSQL:
				sb.append("jdbc:mysql://").append(settings.getIpAddress()).append(":").append(settings.getPort()).append("/").append(settings.getDataBaseName())
						.append("?useUnicode=true&characterEncoding=utf-8&useSSL=false");
				break;
			case POSTGRESQL:
				sb.append("jdbc:postgresql://").append(settings.getIpAddress()).append(":").append(settings.getPort()).append("/").append(settings.getDataBaseName());
				break;
			case ORACLE:
				sb.append("jdbc:oracle:thin:@").append(settings.getIpAddress()).append(":").append(settings.getPort()).append(":").append(settings.getDataBaseName());
				break;
			case HIVE:
				sb.append("jdbc:hive2://").append(settings.getIpAddress()).append(":").append(settings.getPort()).append("/").append(settings.getDataBaseName());
				break;
			default:
				break;
		}
		return sb.toString();
	}

	/**
	 * 生成数据库url
	 * 
	 * @param databaseName 数据库名
	 * @param ip           数据库IP地址
	 * @param port         数据库端口
	 * @param databaseType 数据库类型：MYSQL/POSTGRESQL
	 * @return
	 */
	public static String generateDataBaseUrl(String databaseName, String ip, String port, String databaseType) {
		StringBuilder sb = new StringBuilder("");
		switch (databaseType) {
			case "MYSQL":
				sb.append("jdbc:mysql://").append(ip).append(":").append(port).append("/").append(databaseName).append("?useUnicode=true&characterEncoding=utf-8&useSSL=false");
				break;
			case "POSTGRESQL":
				sb.append("jdbc:postgresql://").append(ip).append(":").append(port).append("/").append(databaseName);
				break;
			case "ORACLE":
				sb.append("jdbc:oracle:thin:@").append(ip).append(":").append(port).append(":").append(databaseName);
				break;
			case "HIVE":
				sb.append("jdbc:hive2://").append(ip).append(":").append(port).append("/").append(databaseName);
				break;
			default:
				break;
		}
		return sb.toString();
	}

}
