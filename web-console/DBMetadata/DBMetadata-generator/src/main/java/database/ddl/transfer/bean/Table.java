package database.ddl.transfer.bean;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 表结构定义
 *
 * @author gs
 */
public class Table {
	/**
	 * 表名
	 */
	private String tableName;

	/**
	 * 表排序规则
	 */
	private String tableCollation;

	/**
	 * 表注释描述
	 */
	private String tableComment;

	/**
	 * 列定义
	 */
	private List<Column> columns;

	/**
	 * 主键定义
	 */
	private PrimaryKey primaryKey;

	/**
	 * map数据结构的column集合
	 */
	private Map<String, Column> columnsMap;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public PrimaryKey getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(PrimaryKey primaryKey) {
		this.primaryKey = primaryKey;
	}

	/**
	 * 添加列定义(List结构)
	 * 
	 * @param column 列定义
	 */
	public void addColumn(Column column) {
		if (this.columns == null) {
			this.columns = new ArrayList<>();
		}
		this.columns.add(column);
	}

	public String getTableCollation() {
		return tableCollation;
	}

	public void setTableCollation(String tableCollation) {
		this.tableCollation = tableCollation;
	}

	public String getTableComment() {
		return tableComment;
	}

	public void setTableComment(String tableComment) {
		this.tableComment = tableComment;
	}

	public Map<String, Column> getColumnsMap() {
		return columnsMap;
	}

	public void setColumnsMap(Map<String, Column> columnsMap) {
		this.columnsMap = columnsMap;
	}

	/**
	 * 添加列定义(Map结构)
	 * 
	 * @param column 列定义
	 */
	public void putColumn(Column column) {
		if (this.columnsMap == null) {
			this.columnsMap = new LinkedHashMap<String, Column>();
		}
		this.columnsMap.put(column.getColumnName(), column);
	}
	
}
