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

/**
 * PostgreSQL数据库结构分析
 *
 * @author gs
 */
public class PostgreSqlAnalyser extends Analyser {

	private final String CONSTRAINT_NAME_PRIMARY_KEY = "p";

	/**
	 * 构造方法
	 *
	 * @param connection 数据库连接
	 */
	public PostgreSqlAnalyser(Connection connection) {
		super(connection);
	}

	@Override
	protected List<PrimaryKey> getPrimaryKeyDefines(Connection connection, String catalog, String schema) {
		String sql = "select pg_class.relname as table_name,pg_attribute.attname as column_name,pg_constraint.conname as pk_name from pg_constraint "
				+ "inner join pg_class on pg_constraint.conrelid = pg_class.oid inner join pg_attribute on pg_attribute.attrelid = pg_class.oid and "
				+ "pg_attribute.attnum = any(pg_constraint.conkey) where pg_constraint.contype=?";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<PrimaryKey> primaryKeyList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, CONSTRAINT_NAME_PRIMARY_KEY);
			resultSet = preparedStatement.executeQuery();

			PrimaryKey primaryKey = null;
			Map<String, PrimaryKey> primaryKeyMap = new HashMap<>();
			String tableName = null;
			String pkName = null;
			while (resultSet.next()) {
				tableName = resultSet.getString("table_name").toLowerCase();
				pkName = resultSet.getString("pk_name");
				primaryKey = primaryKeyMap.get(tableName);
				if (primaryKey == null) {
					primaryKey = new PrimaryKey();
					primaryKey.setTableName(tableName);
					primaryKey.setPkName(pkName);

					primaryKeyList.add(primaryKey);
					primaryKeyMap.put(tableName, primaryKey);
				}
				primaryKey.addColumn(resultSet.getString("column_name").toLowerCase());
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取postgresql表主键定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		return primaryKeyList;
	}

	@Override
	protected List<Column> getColumnDefines(Connection connection, String catalog, String schema) {
		String sql = "select c.relname,a.attname,case a.atttypid when 21 /*int2*/ then 16 when 23 /*int4*/ then 32 when 20 /*int8*/ then 64 when 1700 /*numeric*/ then case when atttypmod = -1 then null else ((atttypmod - 4) >> 16) & 65535  end " + 
				"when 700 /*float4*/ then 24 /*FLT_MANT_DIG*/ when 701 /*flaot8*/ then 53 /*DBL_MANT_DIG*/ else null end as numeric_precision,case when atttypid in (21, 23, 20) then 0 when atttypid in (1700) then " + 
				"case when atttypmod = -1 then null else (atttypmod - 4) & 65535 end else null end as numeric_scale,t.typname,a.attnum,case when position('char' in t.typname) > 0 then (a.atttypmod-4) else null end AS lengthvar," + 
				"a.attnotnull AS notnull,n.adsrc,b.description AS comment,q.contype from pg_class c,pg_attribute a " + 
				"left outer join pg_description b ON a.attrelid = b.objoid AND a.attnum = b.objsubid " + 
				"left outer join pg_attrdef n ON a.attrelid = n.adrelid and a.attnum = n.adnum left outer join (select p.oid,o.contype,o.conkey from pg_constraint o inner join pg_class p  on p.oid = o.conrelid) q on q.oid = a.attrelid and a.attnum = any(q.conkey),pg_type t,pg_tables m " + 
				"WHERE c.relname = m.tablename and m.schemaname = 'public' " + 
				"and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid " + 
				"ORDER BY c.relname,a.attnum";

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<Column> columnList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			Column column = null;
			while (resultSet.next()) {
				column = this.recordColumn(resultSet);
				columnList.add(column);
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取postgresql表字段定义失败", e);
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
		column.setDataBaseType(DataBaseType.POSTGRESQL);
		column.setTableName(resultSet.getString("relname").toLowerCase());
		column.setColumnName(resultSet.getString("attname").toLowerCase());
		column.setColumnComment(resultSet.getString("comment"));
		column.setColumnOrder(resultSet.getInt("attnum"));
		column.setDefaultDefine(resultSet.getString("adsrc"));
		column.setNullAble(!"t".equalsIgnoreCase(resultSet.getString("notnull")));
//		column.setColumnType(resultSet.getString("typname"));
		column.setColumnKey(resultSet.getString("contype"));
//		column.setExtra(resultSet.getString("adsrc"));
		column.setDataType(resultSet.getString("typname"));

		if (column.notTextType() && column.notBlobType() && column.notClobType()) {
			if (resultSet.getObject("numeric_precision") != null) {
				column.setPrecision(resultSet.getInt("numeric_precision"));
			} else if (resultSet.getObject("lengthvar") != null) {
				column.setStrLength(resultSet.getInt("lengthvar"));
			}

			if (resultSet.getObject("numeric_scale") != null) {	
				column.setScale(resultSet.getInt("numeric_scale"));
			}
		}

		return column;
	}

	@Override
	protected List<Table> getTableDefines(Connection connection, String catalog, String schema) {
		//  由于pg库9.0以下版本，不存在默认排序规则字段，所以不查询该值
		String sql = "select a.tablename as table_name,c.description as table_comment from pg_tables a, pg_class b left join pg_description c on b.oid = c.objoid and c.objsubid = '0' where " + 
				"a.tablename = b.relname and a.tablename not like 'pg%' and a.tablename not like 'sql_%' and a.schemaname = 'public' order by a.tablename";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<Table> tableList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				Table table = new Table();
				table.setTableName(resultSet.getString("table_name").toLowerCase());
				table.setTableComment(resultSet.getString("table_comment"));
				tableList.add(table);
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取postgresql表定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}

		return tableList;
	}

	@Override
	protected DataBaseDefine getDataBaseDefines(Connection connection) {
		// 由于pg库9.0以下版本，不存在默认排序规则字段datcollate，所以不查询该值
		String sql = "select datname,pg_encoding_to_char(encoding) as charater_set_database from pg_database where datname = ?";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		DataBaseDefine dataBaseDefine = new DataBaseDefine();
		try {
			String catalog = connection.getCatalog();
			dataBaseDefine.setCatalog(catalog);
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, catalog);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				String charaterSet = resultSet.getString("charater_set_database");
				dataBaseDefine.setCharacterSetDataBase(charaterSet);
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取postgresql库定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		
		return dataBaseDefine;
	}
}
