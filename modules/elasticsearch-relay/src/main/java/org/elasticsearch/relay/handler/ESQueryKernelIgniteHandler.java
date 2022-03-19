package org.elasticsearch.relay.handler;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryFieldsMetaResult;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryResult;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.ESUtil;
import org.elasticsearch.relay.util.HttpUtil;
import org.elasticsearch.relay.util.SqlTemplateParse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;


/**
 * Central query handler splitting up queries between multiple ignite instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryKernelIgniteHandler extends ESQueryHandler {
	GridKernalContext ctx;
	
	public ESQueryKernelIgniteHandler(ESRelayConfig config,GridKernalContext ctx) throws Exception{
		super(config);
		this.ctx = ctx;
		//then
		if(ctx!=null){
			
		}
		
	}
	
	private Ignite ignite(int index) {
		try {
			if(index==0) {
			   return Ignition.ignite(config.getClusterName());			
			}
			else if(index==1) {
			   return Ignition.ignite(config.getEs2ClusterName());	
			}	
			throw new IndexOutOfBoundsException(); 
		}
		catch(IgniteIllegalStateException e) {			
			fLogger.log(Level.SEVERE, "无法找到ignite集群", e);
			throw e; 
		}
	}
	
	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {				
		super.destroy();
	}
	
	/**
	 * 返回cacheName，如果是表，返回SQL_SCHEMA_TABLE
	 * @param path
	 * @return
	 */
	protected String cacheName(Ignite ignite,String... path){		
		String schema = path[0];
		String table = path[1];		
		if(table.length()==0){ // 只有一个路径片，则是cacheName
			return schema;
		}				
		for(String name: ignite.cacheNames()) {
			if(name.equalsIgnoreCase(schema)) {
				return name;
			}
		}
		String cacheName = "SQL_"+schema+"_"+table.toUpperCase();		
		return cacheName;
	}
	
	/**
	 * 返回cache的typeName，如果不存在，返回defaultType
	 * @param path
	 * @return
	 */
	protected String typeName(IgniteCache<?,?> cache,String defaultType){		
		CacheConfiguration<?,?> config = cache.getConfiguration(CacheConfiguration.class);
		String table = defaultType;	
		
		if(config.getQueryEntities().size()==1){
			return config.getQueryEntities().iterator().next().getValueType();
		}				
		if(config.getQueryEntities().size()==0){
			return table;
		}
		else {			
			for(QueryEntity ent: config.getQueryEntities()) {
				if(ent.getTableName().equals(defaultType)) {
					return ent.getValueType();
				}
			}
		}		
		return table;
	}
	
	
	@Override
	protected Object sendEsRequest(ESUpdate query, int index) throws Exception {
		Ignite ignite = ignite(index);
		String[] path = query.getQueryPath();
		String cacheName = cacheName(ignite,path);
		IgniteCache<?,?> cache0 = ignite.cache(cacheName);
		if(cache0==null) {
			throw new CacheServerNotFoundException(cacheName);
		}
		
		String typeName = typeName(cache0,path[1]);
		String key = path[2];
		
		boolean rv = false;
		
		ObjectNode jsonResq = new ObjectNode(ESRelay.jsonNodeFactory);
		
		IgniteCache<String, BinaryObject> cache = cache0.withKeepBinary();	
		
		//将json转为binaryObject
		if(query.getOp().equals(ESConstants.INSERT_FRAGMENT)){			
			
			BinaryObject bobj = ESUtil.jsonToBinaryObject(ignite,typeName,query.getQuery());
			
			rv = cache.putIfAbsent(key, bobj);
			if(rv){
				jsonResq.put("_index", path[0]);
				jsonResq.put("_type", path[1]);
				jsonResq.put("_id", path[2]);
				jsonResq.put("result", "created");
			}
			else{
				jsonResq.put("result", "existed");
			}
			return jsonResq;
		}
		else if(query.getOp().equals(ESConstants.UPDATE_FRAGMENT)){
			
			BinaryObject bobj = ESUtil.jsonToBinaryObject(ignite,typeName,query.getQuery());
			
			cache.put(key, bobj);
			
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);
			jsonResq.put("_id", path[2]);
			jsonResq.put("result", "updated");
			return jsonResq;
			
		}
		else if(query.getOp().equals(ESConstants.DELETE_FRAGMENT)){			
			
			cache.remove(key);
			
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);
			jsonResq.put("_id", path[2]);
			jsonResq.put("result", "deleted");
			return jsonResq;
			
		}
		else if(query.getOp().equals(ESConstants.BULK_FRAGMENT)){
			ArrayNode list = query.getQuery().withArray(ESConstants.BULK_FRAGMENT);
			
			// TODO @byron
			for(JsonNode o: list) {
				BinaryObject bobj = ESUtil.jsonToBinaryObject(ignite,typeName,(ObjectNode)o);
				String rowKey = o.get(key).asText();
				cache.put(rowKey, bobj);
			}
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);			
			jsonResq.put("result", list.size());
			return jsonResq;
		}

		// replace spaces since they cause problems with proxies etc.
		
		query.setFormat("form");
		return super.sendEsRequest(query, index);
	}
	
	@Override
	protected Object sendEsRequest(ESQuery query, int index) throws Exception {			
		Ignite ignite = ignite(index);		
		String[] path = query.getQueryPath();
		String cacheName = cacheName(ignite,path);		
		IgniteCache<?,?> cache0 = ignite.cache(cacheName);
		if(cache0==null) {
			throw new CacheServerNotFoundException(cacheName);
		}
		
		String typeName = typeName(cache0,path[1]);			
		IgniteCache<?, BinaryObject> cache = cache0.withKeepBinary();	
		
		
		
		String[] keywords = query.getParams().get("q");
		String keyword = "";
		if(keywords==null){
			ObjectNode queryObj = query.getQuery().with("query");
			if(queryObj.has("multi_match")){
				ObjectNode multi_match = queryObj.with("multi_match");
				ArrayNode fields = multi_match.withArray("fields");
				String queryString = multi_match.get("query").asText();
				keyword = "";
				for(int i=0;i<fields.size();i++){
					keyword+=fields.get(i)+":"+queryString+" ";
				}
			}
		}
		else {
			keyword = String.join(" ", keywords);
		}
		TextQuery<Object, BinaryObject> qry = new TextQuery<>(typeName, keyword);
		
		for(Map.Entry<String, String[]> param: query.getParams().entrySet()) {
			if(param.getKey().equals("pageSize")) {
				String[] pageSize = query.getParams().get("pageSize");
				if(pageSize!=null){
					qry.setPageSize(Integer.valueOf(pageSize[0]));
				}
			}			
		}
		
        QueryCursor<Cache.Entry<Object, BinaryObject>> qryCur = cache.query(qry);
        
        List<Cache.Entry<Object, BinaryObject>> list = qryCur.getAll();
		
		CacheQueryResult res = new CacheQueryResult();
		res.setItems(list);
		
		List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
        res.setFieldsMetadata(convertMetadata(fieldsMeta));

        return res;
		
	}
	
	@Override
	protected Object sendEsRequest(ESViewQuery query, int index) throws Exception {
		Ignite ignite = ignite(index);	
		String[] path = query.getQueryPath();
		
		IgniteEx igniteEx = (IgniteEx) ignite;
		
		
		SqlFieldsQuery qry = new SqlFieldsQuery(query.buildSQL());
		qry.setSchema(query.getSchema());
		qry.setPageSize(query.getPageSize());
		qry.setCollocated(true);
		
        QueryCursor<List<?>> qryCur = igniteEx.context().query().querySqlFields(qry,true);

        List<List<?>>  list = qryCur.getAll();        
        List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
        
        if(query.getResponseFormat()==ResponseFormat.FIELDSMETA) {
        	CacheQueryResult res = new CacheQueryResult();        	
            res.setFieldsMetadata(fieldsMeta);
        	res.setItems(list);
        	return res;
        }
		
        List<ObjectNode> dataset = new ArrayList<>(list.size());
        
        List<SubSqlQueryCallable> tasks = new LinkedList<>();
        for(List<?> row: list) {
        	ObjectNode node = ESUtil.getObjectNode(row, fieldsMeta);
			dataset.add(node); 
			
	        if(query.getNamedSQL()!=null && !query.getNamedSQL().isEmpty()) {
	        	// 并行执行	        	
	        	for(String fieldName: query.getNamedSQL().keySet()) {
	        		String namedSql = query.getNamedSQL().get(fieldName);
	        		// 注释掉，则不返回
	        		if(!namedSql.startsWith("#") && namedSql.length()>1) { 	
	        			
	        			SubSqlQueryCallable  task = new SubSqlQueryCallable(igniteEx.context(),namedSql,node,fieldName);
	        			tasks.add(task); 
	        		}	        		
	        	} 	
	        }
        }
        
        List<Future<ArrayNode>> results = ctx.pools().getExecutorService().invokeAll(tasks);
    	for(Future<ArrayNode> f: results) {
    		f.get();
    	}   
          
        return dataset;
	}
	
	
	protected int _addQueryResultResponses(List<ObjectNode> hits,Object es1Response, int limit) throws Exception {
		
		if(es1Response instanceof CacheQueryResult) {
			CacheQueryResult result = (CacheQueryResult) es1Response;			

			// mix results 50:50 as far as possible
			Iterator<List<?>> es1Hits = (Iterator)result.getItems().iterator();
			
			List<GridQueryFieldMetadata> fieldsMeta = (List)result.getFieldsMetadata();			
			
			while (es1Hits.hasNext() && hits.size() < limit) {
				ObjectNode node = ESUtil.getObjectNode(es1Hits.next(), fieldsMeta);
				if (node!=null) {					
					addHit(hits, node);
				}				
			}
			return result.getItems().size()>0? 1: 0;
		}
		if(es1Response instanceof List) {
			List<ObjectNode> result = (List) es1Response;			

			// mix results 50:50 as far as possible
			Iterator<ObjectNode> es1Hits = result.iterator();
			
			while (es1Hits.hasNext() && hits.size() < limit) {
				ObjectNode node = es1Hits.next();
				if (node!=null) {					
					addHit(hits, node);
				}				
			}
			return result.size()>0? 1: 0;
		}
		return 0;
		
	}
	
	@Override
	protected String mergeQueryResultResponses(Object es1Response, Object es2Response, int limit, ResponseFormat format) throws Exception {
		
		if(es1Response instanceof CacheQueryResult || es2Response instanceof CacheQueryResult) {
			CacheQueryResult result = (CacheQueryResult) es1Response;
			CacheQueryResult result2 = (CacheQueryResult) es2Response;			
			
			CacheQueryResult mergedResult = null;
			if(result!=null) {
				mergedResult = result;
				mergedResult.setFieldsMetadata(convertMetadata((Collection)result.getFieldsMetadata()));
			}
			if(result2!=null) {
				if(mergedResult==null) {
					mergedResult = result2;
					mergedResult.setFieldsMetadata(convertMetadata((Collection)result2.getFieldsMetadata()));
				}
				else {					
					mergedResult.getItems().addAll((Collection)result2.getItems());
				}
			}
			
			GridRestResponse resultResponse = new GridRestResponse(mergedResult);   
				
			String esResponse = ESRelay.objectMapper.writeValueAsString(resultResponse);
			return esResponse;
			
					
		}
		else if(es1Response instanceof List || es2Response instanceof List) {
			List<ObjectNode> result = (List) es1Response;
			List<ObjectNode> result2 = (List) es2Response;			
			
			
			List<ObjectNode> hits = new LinkedList<>();
			int shard = _addQueryResultResponses(hits,result,limit);
			shard+=_addQueryResultResponses(hits,result2,limit);
			
			int total = 0;
			if(result!=null) total += result.size();
			if(result2!=null) total += result2.size();
			// add up data
			ESResponse mergedResponse = new ESResponse(hits);
			mergedResponse.setShards(shard);
			mergedResponse.setTotalHits(total);
			
			if(format==ResponseFormat.DATASET) {
				return mergedResponse.toDataset("items").toPrettyString();
			}     
			else {
				return mergedResponse.toJSON().toPrettyString();	
			}
					
		}
		
		if(es2Response==null && es1Response==null){
			throw new Exception("ES 1.x error: " + "both null result");
		}		

		return super.mergeQueryResultResponses(es1Response, es2Response, limit, format);
	}

	 /**
     * @param meta Internal query field metadata.
     * @return Rest query field metadata.
     */
    private Collection<CacheQueryFieldsMetaResult> convertMetadata(Collection<GridQueryFieldMetadata> meta) {
        List<CacheQueryFieldsMetaResult> res = new ArrayList<>();

        if (meta != null) {
            for (GridQueryFieldMetadata info : meta)
                res.add(new CacheQueryFieldsMetaResult(info));
        }

        return res;
    }
    
   
    
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
			try {
				Object value = parser.getValue(row);
				if(value!=null) {
					sqlStr = value.toString();
				}
				SqlFieldsQuery subQry = new SqlFieldsQuery(sqlStr);
				QueryCursor<List<?>> subQryCur = ctx.query().querySqlFields(subQry,true);  
				
				List<List<?>> subList = subQryCur.getAll();
				ArrayNode list = null;
				synchronized(row){
					list = row.withArray(column);
				}
				for(List<?> rowOne: subList) {				 
				  List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)subQryCur).fieldsMeta();
				  ObjectNode node = ESUtil.getObjectNode(rowOne, fieldsMeta);
				  list.add(node);
				}				
				return list;
			}
			catch(Exception e) {
				fLogger.warning(e.getMessage());
			}			
			return null;
		}
    	
    }
}
