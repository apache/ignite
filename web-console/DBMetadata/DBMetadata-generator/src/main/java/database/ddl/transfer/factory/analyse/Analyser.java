package database.ddl.transfer.factory.analyse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * 数据库分析接口
 *
 * @author gs
 */
public abstract class Analyser {
	protected Connection connection;

	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * 构造方法
	 * 
	 * @param connection 数据库连接
	 */
	public Analyser(Connection connection) {
		this.connection = connection;
	}

	/**
	 * 获取数据库中元素定义信息
	 * 
	 * @param connection 数据库连接
	 * @return 数据库中元素定义信息
	 */
	public DataBaseDefine getDataBaseDefine(Connection connection) throws SQLException {
		DataBaseDefine dataBaseDefine = this.getDataBaseDefines(connection);
		List<Table> tables = this.listTables(connection);
		for (Table table : tables) {
			dataBaseDefine.putTable(table);
		}
		return dataBaseDefine;
	}

	/**
	 * 列出表定义
	 * 
	 * @param connection 数据库连接
	 * @return 表定义
	 * @throws SQLException
	 */
	protected List<Table> listTables(Connection connection) throws SQLException {
		String catalog = connection.getCatalog();
		String schema = connection.getSchema();
		logger.info("开始读取数据库表结构定义，数据库名：{}，模式：{}", catalog, schema);

		logger.info("开始读取表信息");
		List<Table> tableDefinesList = this.getTableDefines(connection, catalog, schema);
		logger.info("获取到{}条表信息", tableDefinesList.size());
		Map<String, Table> tableMap = new HashMap<>();
		
		logger.info("开始处理表信息");
		if(tableDefinesList != null) {
			for (Table table : tableDefinesList) {
				tableMap.put(table.getTableName(), table);
			}
		}
		logger.info("表信息处理完成");
		

		logger.info("开始读取列信息");
		List<Column> columnList = this.getColumnDefines(connection, catalog, schema);
		this.filterDuplicateColumn(columnList);
		logger.info("获取到{}条列信息", columnList.size());

		logger.info("开始归并列至所属表");
		if (columnList != null) {
			String tableName = null;
			Table table = null;
			for (Column column : columnList) {
				tableName = column.getTableName();

				table = tableMap.get(tableName);
				if (table == null) {
					table = new Table();
					table.setTableName(tableName);
					tableDefinesList.add(table);
					tableMap.put(tableName, table);
				}
				table.addColumn(column);
				table.putColumn(column);
			}
		}
		logger.info("归并列至所属表完成");

		logger.info("开始获取主键信息");
		List<PrimaryKey> primaryKeyList = this.getPrimaryKeyDefines(connection, catalog, schema);
		logger.info("主键信息获取完成");

		logger.info("开始归并主键信息");
		this.mergePrimaryKey(tableMap, primaryKeyList);
		logger.info("主键信息归并完成");

		logger.info("数据库表结构定义读取完毕，共读取到{}条表信息", tableDefinesList.size());
		return tableDefinesList;
	}

	/**
	 * 过滤重复的字段定义，针对mysql坑爹的加前后空白认为是不重名字段定义的问题
	 * 
	 * @param columnList 字段定义
	 */
	private void filterDuplicateColumn(List<Column> columnList) {
		if (columnList != null) {
			Map<String, String> columnFilterMap = new HashMap<>();
			Iterator<Column> iterator = columnList.iterator();
			Column column = null;
			String key = null;

			while (iterator.hasNext()) {
				column = iterator.next();
				key = column.getTableName().toLowerCase().trim() + "@" + column.getColumnName().toLowerCase().trim();

				if (columnFilterMap.get(key) != null) {
					iterator.remove();
					logger.warn("发现重复字段定义，表名：{}，列名：{}", column.getTableName(), column.getColumnName());
				} else {
					columnFilterMap.put(key, key);
				}
			}
		}
	}

	/**
	 * 归并主键信息至表定义中
	 * 
	 * @param tableMap       表定义
	 * @param primaryKeyList 主键信息
	 */
	private void mergePrimaryKey(Map<String, Table> tableMap, List<PrimaryKey> primaryKeyList) {
		if (primaryKeyList != null) {
			Table table = null;
			String tableName = null;
			for (PrimaryKey primaryKey : primaryKeyList) {
				tableName = primaryKey.getTableName();
				table = tableMap.get(tableName);

				if (table != null) {
					table.setPrimaryKey(primaryKey);
				}
			}
		}
	}

	/**
	 * 获取主键定义
	 * 
	 * @param connection 数据库连接
	 * @param catalog    数据库名
	 * @param schema     模式
	 * @return 列定义
	 */
	protected abstract List<PrimaryKey> getPrimaryKeyDefines(Connection connection, String catalog, String schema);

	/**
	 * 获取列定义
	 * 
	 * @param connection 数据库连接
	 * @param catalog    数据库名
	 * @param schema     模式
	 * @return 列定义
	 */
	protected abstract List<Column> getColumnDefines(Connection connection, String catalog, String schema);
	
	/**
	 * 获取数据库定义
	 * 
	 * @param connection 数据库连接
	 * @return 数据库定义
	 */
	protected abstract DataBaseDefine getDataBaseDefines(Connection connection);
	
	/**
	 * 获取数据库定义
	 * 
	 * @param connection 数据库连接
	 * @param catalog    数据库名
	 * @param schema     模式
	 * @return 列定义
	 */
	protected abstract List<Table> getTableDefines(Connection connection, String catalog, String schema);

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
}
