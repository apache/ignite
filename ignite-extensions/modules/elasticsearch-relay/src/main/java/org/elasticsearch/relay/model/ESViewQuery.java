package org.elasticsearch.relay.model;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.util.SqlTemplateParse;


/**
 * 查询视图，可以包含多个sql，支持?参数
 * @author WBPC1158
 *
 */
public class ESViewQuery {
	private ResponseFormat responseFormat = ResponseFormat.HITS; // json, dataset, tri-tuple
	private String SQL;
	private String schema;
	private int pageSize = 100;

	private Map<String,String> namedSQL;
	
	private String format = "json";
	
	private String[] fPath;

	private Map<String, String[]> fParams;
	
	private boolean fCancelled = false;
	
	public ESViewQuery() {
		
	}
	
	public ESViewQuery(String sql) {
		this.SQL = sql;
	}

	public ESViewQuery(String schema, String sql) {
		this.SQL = sql;
		this.schema = schema;
	}
	
	public String getSQL() {
		return SQL;
	}
	public void setSQL(String sQL) {
		SQL = sQL;
	}
	
	public String getSchema() {
		return schema;
	}

	public void setSchema(String searchPath) {
		this.schema = searchPath;
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

	public Map<String, String[]> getParams() {
		return fParams;
	}
	
	public String param(String... name) {
		for(String nameOne: name) {
			String[] value = fParams.get(nameOne);
			if(value!=null) {
				return value[0];
			}
		}
		return null;
	}

	public void setParams(Map<String, String[]> params) {
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

	public ResponseFormat getResponseFormat() {
		return responseFormat;
	}

	public void setResponseFormat(ResponseFormat responseFormat) {
		this.responseFormat = responseFormat;
	}
	

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	

	/**
	 * @return reassembled query URL (without the server)
	 */
	public String buildQueryURL() {
		StringBuffer urlBuff = new StringBuffer();

		// reconstruct request path
		if (fPath != null) {
			for (String frag : fPath) {
				// skip empty elements
				if (!frag.isEmpty()) {
					urlBuff.append(frag);
					urlBuff.append("/");
				}
			}
		}
		urlBuff.append("?");
		if(SQL !=null) {			
			urlBuff.append("q=");
			try {
				urlBuff.append(URLEncoder.encode(SQL,"utf-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// add parameters
		if (fParams != null && !fParams.isEmpty()) {
			// construct URL with all parameters
			Iterator<Entry<String, String[]>> paramIter = fParams.entrySet().iterator();
			Entry<String, String[]> entry = null;
			
			while (paramIter.hasNext()) {
				entry = paramIter.next();				
				for(String v: entry.getValue()) {
					urlBuff.append("&");
					urlBuff.append(entry.getKey());
					urlBuff.append("=");
					urlBuff.append(v);
				}
			}
		}

		return urlBuff.toString();
	}
	public String buildSQL() {
		
		StringBuilder where = new StringBuilder();
		
		for(Map.Entry<String, String[]> param: getParams().entrySet()) {
			if(param.getKey().equals("pageSize") || param.getKey().equals("pagePer")) {
				String[] pageSize = param.getValue();
				if(pageSize!=null){
					this.pageSize = Integer.valueOf(pageSize[0]);
				}
			}
			else if(param.getKey().equals("responseFormat")) {
				// have processed
				String[] responseFormat = param.getValue();
				if(responseFormat!=null) {
					this.setResponseFormat(ResponseFormat.valueOf(responseFormat[0].toUpperCase()));
				}
			}
			else if(param.getKey().equals("page") || param.getKey().equals("pageNo")) {
				// not have processed
			}
			else if(param.getKey().charAt(0)!='_'){
				where.append(param.getKey());
				if(param.getValue().getClass().isArray()) {
					where.append(" IN ");
					where.append(SqlTemplateParse.renderString(param.getValue()));
				}
				else {
					where.append(" = ");
					where.append(SqlTemplateParse.renderString(param.getValue()));
				}
			}
		}
		
		String sql = getSQL();
		if(where.length()>1) {
			sql = "select * from ("+sql+") where "+where.toString();
		}
		return sql;
	}


}
