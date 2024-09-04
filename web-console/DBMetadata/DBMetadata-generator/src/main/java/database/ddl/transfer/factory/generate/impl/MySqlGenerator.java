package database.ddl.transfer.factory.generate.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.bean.DBSettings;
import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.PrimaryKey;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.factory.generate.Generator;
import database.ddl.transfer.utils.StringUtil;

/**
 * 针对MySQL的数据库结构生成
 *
 * @author gs
 */
public class MySqlGenerator extends Generator {

	public MySqlGenerator(Connection connection, DataBaseDefine dataBaseDefine, DBSettings targetDBSettings) {
		super(connection, dataBaseDefine, targetDBSettings);
	}

	@Override
	protected String getTableDDL(Table tableDefine) {
		StringBuilder stringBuilder = new StringBuilder("create table ");
		stringBuilder.append("`" + tableDefine.getTableName() + "`").append(" (");

		List<Column> columnList = tableDefine.getColumns();
		for (Column column : columnList) {
			stringBuilder.append(this.getColumnDefineDDL(column));
			stringBuilder.append(",");
		}

		PrimaryKey primaryKey = tableDefine.getPrimaryKey();
		if (primaryKey != null) {
			stringBuilder.append(this.getPrimaryKeyDefineDDL(primaryKey)).append(",");
		}

		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		if (!StringUtil.isBlank(tableDefine.getTableComment())) {
			stringBuilder.append(") COMMENT='").append(tableDefine.getTableComment()).append("';");
		} else {
			stringBuilder.append(");");
		}

		return stringBuilder.toString().toLowerCase();
	}

	/**
	 * 生成主键定义的DDL语句
	 * 
	 * @param primaryKey 主键定义
	 * @return DDL语句
	 */
	private String getPrimaryKeyDefineDDL(PrimaryKey primaryKey) {
		StringBuilder stringBuilder = new StringBuilder("primary key(");
		List<String> columnNames = primaryKey.getColumns();
		for (String columnName : columnNames) {
			stringBuilder.append("`" + columnName + "`").append(",");
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		stringBuilder.append(") USING BTREE");

		return stringBuilder.toString();
	}

	/**
	 * 生成字段定义的DDL语句
	 * 
	 * @param column 字段定义
	 * @return DDL语句
	 */
	private String getColumnDefineDDL(Column column) {
		StringBuilder stringBuilder = new StringBuilder("`" + column.getColumnName() + "`");

		String type = column.getFinalConvertDataType();
		stringBuilder.append(" ");
		stringBuilder.append(type);
		if (!column.isNullAble()) {
			stringBuilder.append(" ").append("not null");
		}

		// 暂时注释掉默认值
//		if (column.hasDefault() && column.getDefaultDefine().contains("now")) {
//			stringBuilder.append(" default ").append("CURRENT_TIMESTAMP");
//		}

		if (!StringUtil.isBlank(column.getColumnComment())) {
			stringBuilder.append(" COMMENT '").append(column.getColumnComment()).append("'");
		}

		return stringBuilder.toString();
	}

	@Override
	protected String getDataBaseDDL(DataBaseDefine dataBaseDefine) {
		String sql = "show databases like '" + dataBaseDefine.getCatalog() + "'";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		boolean flag = false;
		try {
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			if(resultSet.next()) {
				flag = true;
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取mysql表定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		StringBuilder stringBuilder = new StringBuilder("");
		if (!flag) {
			stringBuilder.append("CREATE DATABASE ").append(dataBaseDefine.getCatalog()).append(" DEFAULT CHARSET " + dataBaseDefine.getCharacterSetDataBase().toLowerCase() + ";");
		}

		return stringBuilder.toString();
	}

	@Override
	protected List<String> getModifiedColumnDDL(Table sourceTableDefine, Table targetTableDefine) {
		StringBuilder stringBuilder = null;
		String afterColumn = null;
		List<String> resultList = new LinkedList<>();
		for (Column sourceColumn : sourceTableDefine.getColumns()) {
			stringBuilder =  new StringBuilder("");
			String columnName = sourceColumn.getColumnName();
			Column targetColumn = targetTableDefine.getColumnsMap().get(columnName);
			if (targetColumn == null) {
				// 字段不存在直接添加
				stringBuilder.append("ALTER TABLE `").append(sourceTableDefine.getTableName()).append("` ADD COLUMN `").append(columnName).append("` ").append(sourceColumn.getFinalConvertDataType())
						.append(" ");
				if (!sourceColumn.isNullAble()) {
					stringBuilder.append("NOT NULL");
				} else {
					stringBuilder.append("NULL");
				}

				// 暂时注释掉默认值
//				if (sourceColumn.hasDefault() && sourceColumn.getDefaultDefine().contains("now")) {
//					stringBuilder.append(" default ").append("CURRENT_TIMESTAMP");
//				}

				if (!StringUtil.isBlank(sourceColumn.getColumnComment())) {
					stringBuilder.append(" COMMENT '").append(sourceColumn.getColumnComment()).append("'");
				}
				if (!StringUtil.isBlank(afterColumn)) {
					stringBuilder.append(" after `").append(afterColumn).append("`");
				}
				stringBuilder.append(";");
			} else {
				if (sourceColumn.equals(targetColumn)) {
					continue;
				} else {
					// 由于不同数据库类型转换后与实际查询的类型存在不一致，导致不应该修改类型的字段也会再次执行类型修改操作，表数据量大时影响性能，暂关闭类型修改功能
//					stringBuilder.append("ALTER TABLE ").append("`").append(sourceTableDefine.getTableName()).append("`").append(" MODIFY ").append("`")
//					.append(columnName).append("` ").append(sourceColumn.getFinalConvertDataType()).append(" ");
//					if (!sourceColumn.isNullAble()) {
//						stringBuilder.append("NOT NULL");
//					} else {
//						stringBuilder.append("NULL");
//					}

					// 暂时注释掉默认值
//					if (sourceColumn.hasDefault() && sourceColumn.getDefaultDefine().contains("now")) {
//						stringBuilder.append(" default ").append("CURRENT_TIMESTAMP");
//					}

//					if (!StringUtil.isBlank(sourceColumn.getColumnComment())) {
//						stringBuilder.append(" COMMENT '").append(sourceColumn.getColumnComment()).append("'");
//					}
//					if (!StringUtil.isBlank(afterColumn)) {
//						stringBuilder.append(" after `").append(afterColumn).append("`");
//					}
//					stringBuilder.append(";");
				}
			}
			afterColumn = columnName;
			if(!StringUtil.isBlank(stringBuilder.toString())) {
				resultList.add(stringBuilder.toString());
			}
		}
		return resultList;
	}

}
