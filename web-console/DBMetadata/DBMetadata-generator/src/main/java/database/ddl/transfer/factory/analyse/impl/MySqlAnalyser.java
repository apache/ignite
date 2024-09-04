package database.ddl.transfer.factory.analyse.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.factory.analyse.Analyser;
import database.ddl.transfer.utils.StringUtil;

/**
 * 针对MySql的数据库结构分析
 *
 * @author gs
 */
public class MySqlAnalyser extends Analyser {
	private final String CONSTRAINT_NAME_PRIMARY_KEY = "PRIMARY";

	/**
	 * 构造方法
	 *
	 * @param connection 数据库连接
	 */
	public MySqlAnalyser(Connection connection) {
		super(connection);
	}

	@Override
	protected List<PrimaryKey> getPrimaryKeyDefines(Connection connection, String catalog, String schema) {
		String sql = "select table_name, column_name, ordinal_position from information_schema.key_column_usage where table_schema = ? and constraint_name=?";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<PrimaryKey> primaryKeyList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, catalog);
			preparedStatement.setString(2, CONSTRAINT_NAME_PRIMARY_KEY);
			resultSet = preparedStatement.executeQuery();

			PrimaryKey primaryKey = null;
			Map<String, PrimaryKey> primaryKeyMap = new HashMap<>();
			String tableName = null;
			while (resultSet.next()) {
				tableName = resultSet.getString("table_name").toLowerCase();
				primaryKey = primaryKeyMap.get(tableName);
				if (primaryKey == null) {
					primaryKey = new PrimaryKey();
					primaryKey.setTableName(tableName);

					primaryKeyList.add(primaryKey);
					primaryKeyMap.put(tableName, primaryKey);
				}
				primaryKey.addColumn(resultSet.getString("column_name").toLowerCase());
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取MySql表主键定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		return primaryKeyList;
	}

	@Override
	protected List<Column> getColumnDefines(Connection connection, String catalog, String schema) {
		String sql = "select table_name, column_name, column_comment, ordinal_position, column_default, " + " is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, "
				+ " column_key, column_type, extra from information_schema.columns where table_schema = ? order by table_name, ordinal_position";

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<Column> columnList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, catalog);
			resultSet = preparedStatement.executeQuery();

			Column column = null;
			while (resultSet.next()) {
				column = this.recordColumn(resultSet);
				columnList.add(column);
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取MySql表字段定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}

		return columnList;
	}

	/**
	 * 记录列信息
	 * 
	 * @param resultSet 列信息数据集
	 * @return 列信息
	 */
	private Column recordColumn(ResultSet resultSet) throws SQLException {
		Column column = new Column();
		column.setDataBaseType(DataBaseType.MYSQL);
		column.setTableName(resultSet.getString("table_name").toLowerCase());
		column.setColumnName(resultSet.getString("column_name").toLowerCase());
		column.setColumnComment(resultSet.getString("column_comment"));
		column.setColumnOrder(resultSet.getInt("ordinal_position"));
		column.setDefaultDefine(resultSet.getString("column_default"));
		column.setNullAble(!"NO".equalsIgnoreCase(resultSet.getString("is_nullable")));
		column.setColumnType(resultSet.getString("column_type"));
		column.setColumnKey(resultSet.getString("column_key"));
		column.setExtra(resultSet.getString("extra"));
		column.setDataType(resultSet.getString("data_type"));

		if (column.notTextType() && column.notBlobType() && column.notClobType()) {
			if (resultSet.getObject("numeric_precision") != null) {
				column.setPrecision(resultSet.getInt("numeric_precision"));
			} else if (resultSet.getObject("character_maximum_length") != null) {
				column.setStrLength(resultSet.getInt("character_maximum_length"));
			}

			if (resultSet.getObject("numeric_scale") != null) {	
				column.setScale(resultSet.getInt("numeric_scale"));
			}
		}

		return column;
	}

	@Override
	protected List<Table> getTableDefines(Connection connection, String catalog, String schema) {
		String sql = "select table_name, table_collation, table_comment from information_schema.tables where table_schema = ? order by table_name";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<Table> tableList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, catalog);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				Table table = new Table();
				table.setTableName(resultSet.getString("table_name").toLowerCase());
				table.setTableCollation(resultSet.getString("table_collation"));
				table.setTableComment(resultSet.getString("table_comment"));
				tableList.add(table);
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取MySql表定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}

		return tableList;
	}

	@Override
	protected DataBaseDefine getDataBaseDefines(Connection connection) {
		String sql = "show variables where variable_name = 'character_set_database' or variable_name = 'collation_database'";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		DataBaseDefine dataBaseDefine = new DataBaseDefine();
		try {
			dataBaseDefine.setCatalog(connection.getCatalog());
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				String name = resultSet.getString("variable_name");
				String value = resultSet.getString("value");
				if(!StringUtil.isBlank(name) && "character_set_database".equals(name)) {
					dataBaseDefine.setCharacterSetDataBase(value);
				}else if(!StringUtil.isBlank(name) && "collation_database".equals(name)) {
					dataBaseDefine.setCollationDataBase(value);
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取MySql库定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		
		return dataBaseDefine;
	}
}