package database.ddl.transfer.factory.analyse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.factory.analyse.AnalyserFactory;
import database.ddl.transfer.factory.analyse.impl.OracleSqlAnalyser;
import database.ddl.transfer.factory.analyse.impl.MySqlAnalyser;
import database.ddl.transfer.factory.analyse.impl.PostgreSqlAnalyser;

import java.sql.Connection;

/**
 * 数据库结构分析的工厂类
 *
 * @author gs
 */
public final class AnalyserFactory {
	private static Logger logger = LoggerFactory.getLogger(AnalyserFactory.class);

	public static Analyser getInstance(Connection connection) {
		try {
			if (connection == null || connection.isClosed()) {
				throw new IllegalArgumentException(String.format("无效数据库连接，connection equal null : %s，connection closed : %s", (connection == null), connection.isClosed()));
			}
			String dataBaseType = connection.getMetaData().getDatabaseProductName();
			String dataBaseVersion = connection.getMetaData().getDatabaseProductVersion();

			logger.info("开始构造数据库结构分析，数据库类型：{}，版本：{}", dataBaseType, dataBaseVersion);

			Analyser analyser = null;
			if ("mysql".equalsIgnoreCase(dataBaseType)) {
				analyser = new MySqlAnalyser(connection);
			} else if ("postgreSql".equalsIgnoreCase(dataBaseType)) {
				analyser = new PostgreSqlAnalyser(connection);
			} else if ("oracle".equalsIgnoreCase(dataBaseType)) {
				analyser = new OracleSqlAnalyser(connection);
			} else {
				throw new IllegalArgumentException(String.format("无法识别的数据库类型：%s", dataBaseType));
			}
			logger.info("数据库结构分析构造完毕");
			return analyser;
		} catch (Throwable e) {
            throw new RuntimeException("创建数据库结构分析失败", e);
        }
    }

    /**
     * 无需实例化
     */
    private AnalyserFactory() {
    }
}
