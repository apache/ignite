package org.elasticsearch.relay.handler;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.elasticsearch.relay.util.ESUtil;
import org.elasticsearch.relay.util.SqlTemplateParse;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class SubSqlQueryCallable implements Callable<ArrayNode>{
	final String column;
	final ObjectNode row;
	final String sql;
	final GridKernalContext ctx;

	public SubSqlQueryCallable(GridKernalContext ctx,String sql,ObjectNode row,String column) {
		this.sql = sql;
		this.row = row;
		this.column = column;
		this.ctx = ctx;
	}
	
	@Override
	public ArrayNode call() throws Exception {
		SqlTemplateParse parser = new SqlTemplateParse(sql);
		String sqlStr = sql;
		
		Object value = parser.getValue(row);
		if(value!=null) {
			sqlStr = value.toString();
		}
		SqlFieldsQuery subQry = new SqlFieldsQuery(sqlStr);
		QueryCursor<List<?>> subQryCur = ctx.query().querySqlFields(subQry,true);  
		
		List<List<?>> subList = subQryCur.getAll();
		ArrayNode list = null;
		synchronized(row){
			list = row.withArray("/"+column);
		}
		for(List<?> rowOne: subList) {				 
		  List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)subQryCur).fieldsMeta();
		  ObjectNode node = ESUtil.getObjectNode(rowOne, fieldsMeta);
		  list.add(node);
		}				
		return list;

	}
	
}
