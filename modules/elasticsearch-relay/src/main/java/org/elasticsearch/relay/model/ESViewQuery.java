package org.elasticsearch.relay.model;

import java.util.List;
import java.util.Map;


/**
 * 查询视图，可以包含多个sql，支持?参数
 * @author WBPC1158
 *
 */
public class ESViewQuery {
	
	private String SQL;
	private Map<String,String> namedSQL;
	
	private String format = "json";
	
	private String[] fPath;

	private Map<String, String> fParams;
	
	private boolean fCancelled = false;
	
	public ESViewQuery() {
		
	}
	
	public ESViewQuery(String sql) {
		this.SQL = sql;
	}

	public String getSQL() {
		return SQL;
	}
	public void setSQL(String sQL) {
		SQL = sQL;
	}
	public Map<String, String> getNamedSQL() {
		return namedSQL;
	}
	public void setNamedSQL(Map<String, String> namedSQL) {
		this.namedSQL = namedSQL;
	}
	
	public String[] getQueryPath() {
		return fPath;
	}

	public void setQueryPath(String[] path) {
		fPath = path;
	}

	public Map<String, String> getParams() {
		return fParams;
	}

	public void setParams(Map<String, String> params) {
		fParams = params;
	}

	/**
	 * @return whether this query has been cancelled internally
	 */
	public boolean isCancelled() {
		return fCancelled;
	}

	/**
	 * Cancel this query internally - do not process further and do not send.
	 */
	public void cancel() {
		fCancelled = true;
	}
	
	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

}
