package database.ddl.transfer.factory.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.factory.analyse.AnalyserFactory;
import database.ddl.transfer.utils.DBUrlUtil;
import database.ddl.transfer.utils.StringUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * DDL构造类
 *
 * @author gs
 */
public abstract class Generator {
	/**
	 * 数据库连接
	 */
	protected Connection connection;

	/**
	 * 数据库结构中的元素定义
	 */
	protected DataBaseDefine sourceDataBaseDefine;

	/**
	 * 目标数据库配置参数
	 */
	protected DBSettings targetDBSettings;

	protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	public Generator(Connection connection, DataBaseDefine sourceDataBaseDefine, DBSettings targetDBSettings) {
		this.connection = connection;
		this.sourceDataBaseDefine = sourceDataBaseDefine;
		this.targetDBSettings = targetDBSettings;
	}

	/**
	 * 构造数据库结构
	 * 
	 * @throws SQLException
	 */
	public void generateStructure() throws SQLException {
		boolean createDataBase = this.createDataBase();
		if(!targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)) {
			if (createDataBase || !sourceDataBaseDefine.getCatalog().equals(targetDBSettings.getDataBaseName())) {
				// 创建了新库或则连接库和源库不一样，关闭原有连接，切换新连接
				try {
					connection.close();
				} catch (Exception e) {
					logger.error("由于创建了新库，需关闭原有库连接，出现异常", e);
				}
				// 创建基于新库的连接
				targetDBSettings.setDataBaseName(sourceDataBaseDefine.getCatalog());
				String url = DBUrlUtil.generateDataBaseUrl(targetDBSettings);
				connection = DriverManager.getConnection(url, targetDBSettings.getUserName(), targetDBSettings.getUserPassword());
			}
		}else {
			// Oracle数据库为用户名
			if(createDataBase || !sourceDataBaseDefine.getCatalog().equals(targetDBSettings.getUserName())) {
				// 创建了新库或则连接库和源库不一样，关闭原有连接，切换新连接
				try {
					connection.close();
				} catch (Exception e) {
					logger.error("由于创建了新库，需关闭原有库连接，出现异常", e);
				}
				// 创建基于新库的连接
				targetDBSettings.setUserName(sourceDataBaseDefine.getCatalog());
				targetDBSettings.setUserPassword(sourceDataBaseDefine.getCatalog());
				String url = DBUrlUtil.generateDataBaseUrl(targetDBSettings);
				connection = DriverManager.getConnection(url, targetDBSettings.getUserName(), targetDBSettings.getUserPassword());
			}
		}
		this.createTable(sourceDataBaseDefine.getTablesMap().values());
	}

	/**
	 * 创建库结构
	 * @throws SQLException 
	 */
	protected boolean createDataBase() {
		boolean result = false;
		String dataBaseDDL = this.getDataBaseDDL(sourceDataBaseDefine);
		if (!StringUtil.isBlank(dataBaseDDL)) {
			try (Statement statement = this.connection.createStatement();) {
				statement.execute(dataBaseDDL);
				logger.info("库{}创建成功", sourceDataBaseDefine.getCatalog());
				result = true;
			} catch (Throwable e) {
				logger.error(String.format("创建库失败，DDL：%s", dataBaseDDL), e);
			}
		} else {
			logger.info("库{}已存在，无需再次创建", sourceDataBaseDefine.getCatalog());
		}
		return result;
	}

	/**
	 * 创建表结构
	 * 
	 * @param tableDefines 表结构定义
	 * @throws SQLException 
	 */
	protected void createTable(Collection<Table> sourceTableDefines) throws SQLException {
		Analyser analyser = AnalyserFactory.getInstance(connection);
		DataBaseDefine targetDataBaseDefine = analyser.getDataBaseDefine(connection);
		Set<String> targetTableNames = targetDataBaseDefine.getTablesMap().keySet();
		if (sourceTableDefines != null && !sourceTableDefines.isEmpty()) {
			String tableDDL = null;
			List<String> modifiedColumnDDLList = new LinkedList<>();
			try (Statement statement = this.connection.createStatement();) {
				for (Table sourceTable : sourceTableDefines) {
					String tableName = sourceTable.getTableName();
					if(!targetTableNames.contains(tableName)) {
						tableDDL = this.getTableDDL(sourceTable);
						if(targetDBSettings.getDataBaseType().equals(DataBaseType.ORACLE)) {
							String[] sqls = tableDDL.split(";");
							for (String singleSql : sqls) {
								statement.execute(singleSql);
							}
						}else {
							statement.execute(tableDDL);
						}
						logger.info("表{}创建成功", tableName);
					}else {
						modifiedColumnDDLList = this.getModifiedColumnDDL(sourceTable, targetDataBaseDefine.getTablesMap().get(tableName));
						for (String modifiedColumnDDL : modifiedColumnDDLList) {
							statement.execute(modifiedColumnDDL);
							logger.info("表{}字段修改成功,执行DDL：{}", tableName, modifiedColumnDDL);
						}
					}
				}
			} catch (Throwable e) {
				logger.error(String.format("表创建或修改失败，DDL：%s", StringUtil.isBlank(tableDDL) ? modifiedColumnDDLList.toString() : tableDDL), e);
			}

		}
	}

	/**
	 * 释放数据库操作资源
	 * 
	 * @param preparedStatement PreparedStatement
	 * @param resultSet         ResultSet
	 */
	protected void releaseResources(PreparedStatement preparedStatement, ResultSet resultSet) {
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (Throwable e) {
			}
		}

		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (Throwable e) {
			}
		}
	}

	/**
	 * 获取创建数据库的DDL语句
	 * 
	 * @param dataBaseDefine 数据库结构定义
	 * @return 创建数据库的DDL语句
	 */
	protected abstract String getDataBaseDDL(DataBaseDefine dataBaseDefine);

	/**
	 * 获取创建表的DDL语句
	 * 
	 * @param tableDefine 表结构定义
	 * @return 创建表的DDL语句
	 */
	protected abstract String getTableDDL(Table tableDefine);
	
	/**
	 * 表已存在，比较两个表不同的列，并生成列变化的DDL
	 * 
	 * @param sourceTableDefine 源表
	 * @param targetTableDefine 目标表
	 * @return
	 */
	protected abstract List<String> getModifiedColumnDDL(Table sourceTableDefine, Table targetTableDefine);
}
