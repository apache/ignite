package database.ddl.transfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.hive.HiveTransfer;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.factory.analyse.AnalyserFactory;
import database.ddl.transfer.factory.convert.TypeConvertFactory;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.factory.generate.GeneratorFactory;
import database.ddl.transfer.utils.DBUrlUtil;
import database.ddl.transfer.utils.StringUtil;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库结构及数据转换类
 * 
 * @author gs
 */
public final class Transfer {
	private static Logger logger = LoggerFactory.getLogger(Transfer.class);

	/**
	 * 转换处理
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			logger.info("开始读取配置文件");
			Map<String, String> settingMap = getSetting();
			logger.info("配置文件读取完毕，配置信息：{}", StringUtil.toString(settingMap));

			logger.info("开始连接至数据库");
			try (Connection sourceConnection = getSourceConnection(settingMap); Connection targetConnection = getTargetConnection(settingMap);) {
				logger.info("成功连接至数据库");

				logger.info("开始获取源数据库结构定义");
				Analyser analyser = AnalyserFactory.getInstance(sourceConnection);
				DataBaseDefine dataBaseDefine = analyser.getDataBaseDefine(sourceConnection);
				logger.info("源数据库结构定义获取完毕");

				String sourceDataBaseName = sourceConnection.getMetaData().getDatabaseProductName();
				String targetDataBaseName = targetConnection.getMetaData().getDatabaseProductName();
				if (!sourceDataBaseName.equals(targetDataBaseName)) {
					logger.info("开始转换为目标数据库类型");
					String convertType = sourceDataBaseName.toUpperCase() + "2" + targetDataBaseName.toUpperCase();
					TypeConvertFactory.getInstance(convertType).convert(dataBaseDefine);
					logger.info("目标数据库类型转换完成");
				}

				logger.info("开始构造目标数据库结构");
				DBSettings targetDBSettings = new DBSettings();
				targetDBSettings.setDataBaseName(settingMap.get("target.database.name"));
				targetDBSettings.setDriverClass(settingMap.get("target.driverClass"));
				targetDBSettings.setIpAddress(settingMap.get("target.database.ip"));
				targetDBSettings.setPort(settingMap.get("target.database.port"));
				targetDBSettings.setUserName(settingMap.get("target.user.name"));
				targetDBSettings.setUserPassword(settingMap.get("target.user.password"));
				targetDBSettings.setDataBaseType(DataBaseType.POSTGRESQL);
				Generator generator = GeneratorFactory.getInstance(targetConnection, dataBaseDefine, targetDBSettings);
				generator.generateStructure();
				logger.info("目标数据库结构构造完成");

			} catch (Throwable e) {
				throw e;
			}
		} catch (Throwable e) {
			logger.error("转换数据库失败", e);
		}
	}

	/**
	 * 创建目标数据库连接
	 * 
	 * @param settingMap 配置信息
	 * @return 数据库连接
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private static Connection getTargetConnection(Map<String, String> settingMap) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		String driverClass = settingMap.get("target.driverClass");
		String databaseName = settingMap.get("target.database.name");
		String ip = settingMap.get("target.database.ip");
		String port = settingMap.get("target.database.port");
		String databaseType = settingMap.get("target.database.type");
		String userName = settingMap.get("target.user.name");
		String password = settingMap.get("target.user.password");

		String url = DBUrlUtil.generateDataBaseUrl(databaseName, ip, port, databaseType);

		return getConnection(driverClass, url, userName, password);
	}

	/**
	 * 创建源数据库连接
	 * 
	 * @param settingMap 配置信息
	 * @return 数据库连接
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private static Connection getSourceConnection(Map<String, String> settingMap) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		String driverClass = settingMap.get("source.driverClass");
		String databaseName = settingMap.get("source.database.name");
		String ip = settingMap.get("source.database.ip");
		String port = settingMap.get("source.database.port");
		String databaseType = settingMap.get("source.database.type");
		String userName = settingMap.get("source.user.name");
		String password = settingMap.get("source.user.password");

		String url = DBUrlUtil.generateDataBaseUrl(databaseName, ip, port, databaseType);

		return getConnection(driverClass, url, userName, password);
	}

	/**
	 * 获取数据库连接
	 * 
	 * @param driverClass 驱动名称
	 * @param url         连接地址
	 * @param userName    连接用户名
	 * @param password    连接密码
	 * @return 数据库连接
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws SQLException
	 */
	private static Connection getConnection(String driverClass, String url, String userName, String password)
			throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		Class.forName(driverClass).newInstance();
		return DriverManager.getConnection(url, userName, password);
	}

	/**
	 * 获取数据库连接
	 * 
	 * @param settings 数据库配置
	 * @return 数据库连接
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws SQLException
	 */
	private static Connection getConnection(DBSettings settings) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		Class.forName(settings.getDriverClass()).newInstance();
		String url = DBUrlUtil.generateDataBaseUrl(settings);
		return DriverManager.getConnection(url, settings.getUserName(), settings.getUserPassword());
	}

	/**
	 * 获取配置
	 * 
	 * @return 配置信息
	 */
	private static Map<String, String> getSetting() {
		Map<String, String> settingMap = new HashMap<>();

		try (InputStream inputStream = Transfer.class.getClassLoader().getResourceAsStream("jdbc.properties")) {

			Properties settingProperties = new Properties();
			settingProperties.load(inputStream);

			Iterator<Object> keyIterator = settingProperties.keySet().iterator();
			Object key = null;
			while (keyIterator.hasNext()) {
				key = keyIterator.next();

				if (key != null) {
					settingMap.put(key.toString(), settingProperties.getProperty(key.toString()));
				}
			}

			return settingMap;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 关系型数据库表结构传输 （如果库不存在，则会创建库和所有表；库存在，则只会创建所有表）
	 * 
	 * @param sourceDB 源库配置
	 * @param targetDB 目标库配置
	 * @throws Throwable 异常
	 */
	public static void transferRDBMS(DBSettings sourceDB, DBSettings targetDB) throws Throwable {
		logger.info("开始连接至数据库");
		try (Connection sourceConnection = getConnection(sourceDB); Connection targetConnection = getConnection(targetDB);) {
			logger.info("成功连接至数据库");

			logger.info("开始获取源数据库结构定义");
			Analyser analyser = AnalyserFactory.getInstance(sourceConnection);
			DataBaseDefine dataBaseDefine = analyser.getDataBaseDefine(sourceConnection);
			logger.info("源数据库结构定义获取完毕");

			String sourceDataBaseName = sourceConnection.getMetaData().getDatabaseProductName();
			String targetDataBaseName = targetConnection.getMetaData().getDatabaseProductName();
			if (!sourceDataBaseName.equals(targetDataBaseName)) {
				logger.info("开始转换为目标数据库类型");
				String convertType = sourceDataBaseName.toUpperCase() + "2" + targetDataBaseName.toUpperCase();
				TypeConvertFactory.getInstance(convertType).convert(dataBaseDefine);
				logger.info("目标数据库类型转换完成");
			}

			logger.info("开始构造目标数据库结构");
			Generator generator = GeneratorFactory.getInstance(targetConnection, dataBaseDefine, targetDB);
			generator.generateStructure();
			logger.info("目标数据库结构构造完成");
		} catch (Throwable e) {
			throw e;
		}
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:33
	 * @description 将所有库结构导出导目标hive
	 * @param
	 * @return
	 */
	public static void hiveTransferAllDatabase(String sourceUrl, String sourceUserName, String sourcePassword, String targetUrl, String targetUserName, String targetPassword, String driver)
			throws SQLException {
		HiveTransfer.transferAll(sourceUrl, sourceUserName, sourcePassword, targetUrl, targetUserName, targetPassword, driver);
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:35
	 * @description 增量，将单个库的结构导出目标hive
	 * @param
	 * @return
	 */
	public static void hiveTransferOneDatabase(String sourceUrl, String sourceUserName, String sourcePassword, String targetUrl, String targetUserName, String targetPassword, String driver,
			String databaseName) throws SQLException {
		HiveTransfer.incrementByDatabase(sourceUrl, sourceUserName, sourcePassword, targetUrl, targetUserName, targetPassword, driver, databaseName);
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:41
	 * @description 增量，将单个表的结构导出导目标hive的某个库中
	 * @param
	 * @return
	 */
	public static void hiveTransferOneTable(String sourceUrl, String sourceUserName, String sourcePassword, String targetUrl, String targetUserName, String targetPassword, String driver,
			String databaseName, String tableName) throws SQLException {
		HiveTransfer.incrementByTable(sourceUrl, sourceUserName, sourcePassword, targetUrl, targetUserName, targetPassword, driver, databaseName, tableName);
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:43
	 * @description 增量，将单个表的某个分区加到目标表中
	 * @param
	 * @return
	 */
	public static void hiveTransferOnePartition(String targetUrl, String targetUserName, String targetPassword, String driver, String databaseName, String tableName, String partition)
			throws SQLException {
		HiveTransfer.incrementByPartition(targetUrl, targetUserName, targetPassword, driver, databaseName, tableName, partition);
	}

}
