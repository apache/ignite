package database.ddl.transfer.bean;

import java.util.ArrayList;
import java.util.List;

import database.ddl.transfer.utils.StringUtil;

/**
 * 主键定义
 *
 * @author gs
 */
public class PrimaryKey {

	/**
	 * 主键名
	 */
	private String pkName;

	/**
	 * 主键所属表名
	 */
	private String tableName;

	/**
	 * 主键包含列
	 */
	private List<String> columns;

	public String getPkName() {
		if (StringUtil.isBlank(pkName)) {
			return "pk_" + this.tableName;
		}
		return pkName;
	}

	public void setPkName(String pkName) {
		this.pkName = pkName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	/**
	 * 添加主键列
	 * 
	 * @param columnName 列名
	 */
	public void addColumn(String columnName) {
		if (this.columns == null) {
			this.columns = new ArrayList<>();
		}
		this.columns.add(columnName);
	}
}
