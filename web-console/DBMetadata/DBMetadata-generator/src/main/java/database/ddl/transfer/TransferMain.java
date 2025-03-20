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
import database.ddl.transfer.utils.DBConnUtils;
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
public final class TransferMain {
	private static Logger logger = LoggerFactory.getLogger(TransferMain.class);

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
			
			DBSettings sourceDBSettings = getSourceDBSettings(settingMap);
			DBSettings targetDBSettings = getTargetDBSettings(settingMap);
			
			if(sourceDBSettings.getDataBaseType()!=DataBaseType.HIVE && targetDBSettings.getDataBaseType()!=DataBaseType.HIVE) {
				transferRDBMS(sourceDBSettings, targetDBSettings);
			}
			else {
				if(StringUtil.isBlank(sourceDBSettings.getDataBaseName())){ // all database
					hiveTransferAllDatabase(sourceDBSettings,targetDBSettings);
				}
				else if(!StringUtil.isBlank(sourceDBSettings.getDataBaseName())){ // all database
					hiveTransferOneDatabase(sourceDBSettings,targetDBSettings,sourceDBSettings.getDataBaseName());
				}
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
	static DBSettings getTargetDBSettings(Map<String, String> settingMap) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		DBSettings targetDBSettings = new DBSettings();
		targetDBSettings.setDataBaseName(settingMap.get("target.database.name"));
		targetDBSettings.setDriverClass(settingMap.get("target.driverClass"));
		targetDBSettings.setIpAddress(settingMap.get("target.database.ip"));
		targetDBSettings.setPort(settingMap.get("target.database.port"));
		targetDBSettings.setUserName(settingMap.get("target.user.name"));
		targetDBSettings.setUserPassword(settingMap.get("target.user.password"));
		targetDBSettings.setDataBaseType(DataBaseType.valueOf(settingMap.get("target.database.type")));
		return targetDBSettings;
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
	static DBSettings getSourceDBSettings(Map<String, String> settingMap) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		DBSettings sourceDBSettings = new DBSettings();		

		sourceDBSettings.setDataBaseName(settingMap.get("source.database.name"));
		sourceDBSettings.setDriverClass(settingMap.get("source.driverClass"));
		sourceDBSettings.setIpAddress(settingMap.get("source.database.ip"));
		sourceDBSettings.setPort(settingMap.get("source.database.port"));
		sourceDBSettings.setUserName(settingMap.get("source.user.name"));
		sourceDBSettings.setUserPassword(settingMap.get("source.user.password"));
		sourceDBSettings.setDataBaseType(DataBaseType.valueOf(settingMap.get("source.database.type")));
		return sourceDBSettings;
		
	}
	

	/**
	 * 获取配置
	 * 
	 * @return 配置信息
	 */
	static Map<String, String> getSetting() {
		Map<String, String> settingMap = new HashMap<>();

		try (InputStream inputStream = TransferMain.class.getClassLoader().getResourceAsStream("jdbc.properties")) {

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
		Connection sourceConnection = DBConnUtils.getNewConnection(sourceDB);
		Connection targetConnection = DBConnUtils.getNewConnection(targetDB);
		try {
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
		finally {
			targetConnection.close();
			sourceConnection.close();
		}
	}
	
	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:33
	 * @description 将所有库结构导出导目标hive
	 * @param
	 * @return
	 */
	public static void hiveTransferAllDatabase(DBSettings sourceDB, DBSettings targetDB)
			throws SQLException {
		HiveTransfer.transferAll(sourceDB,targetDB);
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:35
	 * @description 增量，将单个库的结构导出目标hive
	 * @param
	 * @return
	 */
	public static void hiveTransferOneDatabase(DBSettings sourceDB, DBSettings targetDB,String databaseName) throws SQLException {
		HiveTransfer.incrementByDatabase(sourceDB, targetDB, databaseName);
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:41
	 * @description 增量，将单个表的结构导出导目标hive的某个库中
	 * @param
	 * @return
	 */
	public static void hiveTransferOneTable(DBSettings sourceDB, DBSettings targetDB, String databaseName, String tableName) throws SQLException {
		HiveTransfer.incrementByTable(sourceDB, targetDB, databaseName, tableName);
	}

	/**
	 * @author luoyuntian
	 * @date 2020-01-15 17:43
	 * @description 增量，将单个表的某个分区加到目标表中
	 * @param
	 * @return
	 */
	public static void hiveTransferOnePartition(DBSettings sourceDB, DBSettings targetDB, String databaseName, String tableName, String partition)
			throws SQLException {
		HiveTransfer.incrementByPartition(targetDB, databaseName, tableName, partition);
	}
}
