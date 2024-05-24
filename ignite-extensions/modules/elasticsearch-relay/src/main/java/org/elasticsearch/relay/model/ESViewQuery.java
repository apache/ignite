package org.elasticsearch.relay.model;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.SqlTemplateParse;

import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * 查询视图，可以包含多个sql，支持?参数
 * @author WBPC1158
 *
 */
public class ESViewQuery extends ESQuery{	
	
	// GET /_views/{schema}/{name}?q=SQL
	private String SQL;
	private String schema;	
	private String name; // views name
	

	private Map<String,String> namedSQL;	
	
	
	public ESViewQuery() {		
		this.name = "";		
	}
	
	public ESViewQuery(String name) {
		this.name = name;		
	}	
	
	public ESViewQuery(String name, String sql) {
		this.name = name;
		this.SQL = sql;
	}

	public ESViewQuery(String schema, String name, String sql) {
		this.name = name;
		this.SQL = sql;
		this.schema = schema;
	}
	
	public ESViewQuery(ESViewQuery copy) {
		super(copy);
		this.name = copy.name;
		this.SQL = copy.SQL;
		this.schema = copy.schema;		
		this.namedSQL = copy.namedSQL;		
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
	
	public void setSchema(String schema) {
		this.schema = schema;
	}

	public Map<String, String> getNamedSQL() {
		return namedSQL;
	}
	
	public void setNamedSQL(Map<String, String> namedSQL) {
		this.namedSQL = namedSQL;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return reassembled query URL (without the server)
	 */
	public String buildQueryURL() {
		StringBuffer urlBuff = new StringBuffer();

		// reconstruct request path
		if (schema != null) {
			// skip empty elements
			if (!schema.isEmpty()) {
				urlBuff.append("/");
				urlBuff.append(schema);				
			}
		}
		if (name != null) {
			// skip empty elements
			if (!name.isEmpty()) {
				urlBuff.append("/");
				urlBuff.append(name);			
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
		if (this.getParams() != null && !this.getParams().isEmpty()) {
			// construct URL with all parameters
			Iterator<Entry<String, String[]>> paramIter = this.getParams().entrySet().iterator();
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
		int limit = this.getLimit();
		int offset = this.getFrom();
		for(Map.Entry<String, String[]> param: getParams().entrySet()) {
			if(param.getKey().equals("responseFormat")) {
				// have processed
				String[] responseFormat = param.getValue();
				if(responseFormat!=null) {
					this.setResponseFormat(ResponseFormat.valueOf(responseFormat[0].toUpperCase()));
				}
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
			sql = "select * from ("+sql+") where " + where.toString();			
		}
		if(offset > 0) {
			sql += " offset " + offset;
		}
		if(limit > 0) {
			sql += " limit " + limit;
		}
		return sql;
	}


}
