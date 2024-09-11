package database.ddl.transfer.bean;

import java.util.LinkedHashMap;
import java.util.Map;

import database.ddl.transfer.bean.Table;
import database.ddl.transfer.utils.StringUtil;

/**
 * 数据库元素结构定义
 */
public class DataBaseDefine {

	/**
	 * 数据库名
	 */
	private String catalog;

	/**
	 * 数据库字符集
	 */
	private String characterSetDataBase;

	/**
	 * 数据库排序规则
	 */
	private String collationDataBase;

	/**
	 * 表定义
	 */
	private Map<String, Table> tablesMap = new LinkedHashMap<String, Table>();;

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog) {
		if(!StringUtil.isBlank(catalog)) {
			this.catalog = catalog.toLowerCase();
		}else {
			this.catalog = catalog;
		}
	}

	public String getCharacterSetDataBase() {
		return characterSetDataBase;
	}

	public void setCharacterSetDataBase(String characterSetDataBase) {
		this.characterSetDataBase = characterSetDataBase;
	}

	public String getCollationDataBase() {
		return collationDataBase;
	}

	public void setCollationDataBase(String collationDataBase) {
		this.collationDataBase = collationDataBase;
	}

	public Map<String, Table> getTablesMap() {
		return tablesMap;
	}

	public void setTablesMap(Map<String, Table> tablesMap) {
		this.tablesMap = tablesMap;
	}

	/**
	 * 添加表定义(Map结构)
	 * 
	 * @param column 列定义
	 */
	public void putTable(Table table) {
		this.tablesMap.put(table.getTableName(), table);
	}

}
