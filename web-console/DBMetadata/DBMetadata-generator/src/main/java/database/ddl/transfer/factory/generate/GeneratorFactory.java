package database.ddl.transfer.factory.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.factory.generate.GeneratorFactory;
import database.ddl.transfer.factory.generate.impl.OracleSqlGenerator;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.factory.generate.impl.MySqlGenerator;
import database.ddl.transfer.factory.generate.impl.PostgreSqlGenerator;

import java.sql.Connection;

/**
 * DDL构造工厂
 *
 * @author gs
 */
public final class GeneratorFactory {
	private static Logger logger = LoggerFactory.getLogger(GeneratorFactory.class);

	/**
	 * 获取数据库ddl构造类，生成数据库结构
	 * 
	 * @param connection 数据库连接
	 * @return 数据库ddl构造类
	 */
	public static Generator getInstance(Connection connection, DataBaseDefine dataBaseDefine, DBSettings targetSettings) {
		try {
			if (connection == null || connection.isClosed()) {
				throw new IllegalArgumentException(String.format("无效数据库连接，connection equal null : %s，connection closed : %s", (connection == null), connection.isClosed()));
			}

			String dataBaseType = connection.getMetaData().getDatabaseProductName();
			String dataBaseVersion = connection.getMetaData().getDatabaseProductVersion();

			logger.info("开始初始化数据库结构，数据库类型：{}，版本：{}", dataBaseType, dataBaseVersion);

			Generator generator = null;
			if ("mysql".equalsIgnoreCase(dataBaseType)) {
				generator = new MySqlGenerator(connection, dataBaseDefine, targetSettings);
			} else if ("postgreSql".equalsIgnoreCase(dataBaseType)) {
				generator = new PostgreSqlGenerator(connection, dataBaseDefine, targetSettings);
			} else if ("oracle".equalsIgnoreCase(dataBaseType)) {
				generator = new OracleSqlGenerator(connection, dataBaseDefine, targetSettings);
			} else {
				throw new IllegalArgumentException(String.format("无法识别的数据库类型：%s", dataBaseType));
			}

			logger.info("数据库结构初始化完毕");
			return generator;
		} catch (Throwable e) {
			throw new RuntimeException("创建数据库结构失败", e);
		}
	}
}
