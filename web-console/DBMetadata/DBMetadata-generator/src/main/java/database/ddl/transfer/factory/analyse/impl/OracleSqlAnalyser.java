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
 * Oracle数据库结构分析
 * 
 * @author gs
 */
public class OracleSqlAnalyser extends Analyser {
	private final String CONSTRAINT_NAME_PRIMARY_KEY = "P";

	/**
	 * 构造方法
	 * 
	 * @param connection 数据库连接
	 */
	public OracleSqlAnalyser(Connection connection) {
		super(connection);
	}

	@Override
	protected List<PrimaryKey> getPrimaryKeyDefines(Connection connection, String catalog, String schema) {
		String sql = "select table_name, column_name, position from user_cons_columns where constraint_name in (select constraint_name from user_constraints where constraint_type = ?)";
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
			throw new RuntimeException("获取Oracle表主键定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		return primaryKeyList;
	}

	@Override
	protected List<Column> getColumnDefines(Connection connection, String catalog, String schema) {
		String sql = "select a.table_name,a.column_name,b.comments,a.column_id,a.data_default,a.nullable,a.data_type,a.char_length,a.data_precision,a.data_scale,d.constraint_type "
				+ "from user_tab_cols a inner join user_col_comments b on b.table_name = a.table_name and b.column_name = a.column_name left join user_cons_columns c on c.table_name = a.table_name "
				+ "and c.column_name = a.column_name left join user_constraints d on d.table_name = c.table_name and d.constraint_name = c.constraint_name order by a.column_id";
		
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
			throw new RuntimeException("获取Oracle表字段定义失败", e);
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
		column.setDataBaseType(DataBaseType.ORACLE);
		column.setTableName(resultSet.getString("table_name").toLowerCase());
		column.setColumnName(resultSet.getString("column_name").toLowerCase());
		column.setColumnComment(resultSet.getString("comments"));
		column.setColumnOrder(resultSet.getInt("column_id"));
		column.setDefaultDefine(resultSet.getString("data_default"));
		column.setNullAble(!"N".equalsIgnoreCase(resultSet.getString("nullable")));
		column.setColumnType(null);
		column.setColumnKey(resultSet.getString("constraint_type"));
		column.setExtra(null);
		column.setDataType(resultSet.getString("data_type"));

		if (column.notTextType() && column.notBlobType() && column.notClobType()) {
			if (resultSet.getObject("data_precision") != null) {
				column.setPrecision(resultSet.getInt("data_precision"));
			} else if (resultSet.getObject("char_length") != null) {
				column.setStrLength(resultSet.getInt("char_length"));
			}

			if (resultSet.getObject("data_scale") != null) {	
				column.setScale(resultSet.getInt("data_scale"));
			}
		}

		return column;
	}

	@Override
	protected DataBaseDefine getDataBaseDefines(Connection connection) {
		String sql = "select * from nls_database_parameters where parameter = 'NLS_CHARACTERSET' or parameter = 'NLS_SORT'";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		DataBaseDefine dataBaseDefine = new DataBaseDefine();
		try {
			dataBaseDefine.setCatalog(connection.getMetaData().getUserName());
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				String name = resultSet.getString("parameter");
				String value = resultSet.getString("value");
				if(!StringUtil.isBlank(name) && "nls_characterset".equalsIgnoreCase(name)) {
					dataBaseDefine.setCharacterSetDataBase(oracleCharacterSetProcessor(value));
				}else if(!StringUtil.isBlank(name) && "nls_sort".equalsIgnoreCase(name)) {
					dataBaseDefine.setCollationDataBase(value);
				}
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取Oracle库定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}
		
		return dataBaseDefine;
	}
	
	/**
	 * Oracle数据库字符集编码不同于其他数据库，需要进行额外加工转换
	 * 
	 * @param characterSet 字符集
	 * @return
	 */
	private String oracleCharacterSetProcessor(String characterSet) {
		String result = "";
		characterSet = characterSet.toLowerCase();
		if(characterSet.indexOf("utf8") != -1) {
			result = "utf8";
		}else if (characterSet.indexOf("gbk") != -1) {
			result = "gbk";
		}
		return result;
	}

	@Override
	protected List<Table> getTableDefines(Connection connection, String catalog, String schema) {
		// Oracle查询没有字符信息，暂不查询该字段
		String sql = "select table_name,comments from user_tab_comments where table_type = 'TABLE'";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<Table> tableList = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				Table table = new Table();
				table.setTableName(resultSet.getString("table_name").toLowerCase());
				table.setTableComment(resultSet.getString("comments"));
				tableList.add(table);
			}
		} catch (Throwable e) {
			throw new RuntimeException("获取Oracle表定义失败", e);
		} finally {
			this.releaseResources(preparedStatement, resultSet);
		}

		return tableList;
	}

}
