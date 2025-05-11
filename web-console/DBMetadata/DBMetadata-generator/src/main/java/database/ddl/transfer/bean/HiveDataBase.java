package database.ddl.transfer.bean;

import java.util.List;

/**
 * @ClassName HiveDataBase
 * @Description TODO
 * @Author luoyuntian
 * @Date 2020-01-08 14:34
 * @Version
 **/
public class HiveDataBase {
	
	/**
	 * 数据库名
	 */
	private String dataBaseName;
	
	/**
	 * 表名
	 */
	private List<String> tables;

	public String getDataBaseName() {
		return dataBaseName;
	}

	public void setDataBaseName(String dataBaseName) {
		this.dataBaseName = dataBaseName;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}
}
