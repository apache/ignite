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
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;

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
		if(table.length()==0){
			return schema;
		}				
		else if(ignite.cacheNames().contains(schema)){
			return schema;
		}
		else if(ignite.cacheNames().contains(schema.toUpperCase())){
			return schema.toUpperCase();
		}
		String cacheName = "SQL_"+schema+"_"+table.toUpperCase();		
		return cacheName;
	}
	
	protected BinaryObject jsonToBinaryObject(Ignite ignite, String typeName,ObjectNode obj){	
		BinaryObjectBuilder bb = ignite.binary().builder(typeName);
		Iterator<Map.Entry<String,JsonNode>> ents = obj.fields();
	    while(ents.hasNext()){	    
	    	Map.Entry<String,JsonNode> ent = ents.next();
	    	String $key =  ent.getKey();
	    	JsonNode $value = ent.getValue();
			try {
			
				if($value.isContainerNode()){
					Object bValue = jsonToObject($value,0);
					bValue = ignite.binary().toBinary(bValue);
					bb.setField($key, bValue);
				}
				else if($value.isMissingNode() || $value.isNull()){
					bb.setField($key, null);
				}
				else if($value.isPojo()){
					Object bValue = ignite.binary().toBinary(((POJONode)$value).getPojo());
					bb.setField($key, bValue);
				}
				else {					
					Object bValue = jsonToObject($value,0);
					bb.setField($key, bValue);
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    return bb.build();
	}
	
	protected Object jsonToObject(JsonNode json,int depth){
		Object ret = null;
		depth++;
		if(json.isObject()) {
			Map<String,Object> obj = new HashMap<>(json.size());
			Iterator<Map.Entry<String,JsonNode>> ents = json.fields();
			
		    while(ents.hasNext()){	    
		    	Map.Entry<String,JsonNode> ent = ents.next();
		    	String $key =  ent.getKey();
		    	JsonNode $value = ent.getValue();
		    	if(depth<=16) {
		    		Object bValue = jsonToObject($value,depth);
		    		obj.put($key, bValue); 	
		    	}
		    	else {
		    		obj.put($key, $value);
		    	}
		    }
		   
		    ret = obj;
		}
		else if(json.isArray()) {
			ArrayNode array = (ArrayNode) json;
			List<Object> obj = new ArrayList<>(json.size());
			for(int i=0;i<array.size();i++) {
				obj.add(jsonToObject(array.get(i),depth));
			}
			ret = array;
		}
		else if(json.isPojo()) {
			ret = ((POJONode)json).getPojo();
		}
		else if(json.isMissingNode() || json.isNull()) {
			
		}
		else if(json.isNumber()) {
			NumericNode number = (NumericNode) json;
			ret = number.numberValue();
		}
		else if(json.isTextual()) {
			ret = json.asText();
		}
		else if(json.isBinary()) {
			ret = ((BinaryNode)json).binaryValue();
		}
		else if(json.isBoolean()) {
			ret = json.asBoolean();
		}
		else {
			ret = json;
		}
		depth--;		
		return ret;
	}
	
	@Override
	protected Object sendEsRequest(ESUpdate query, int index) throws Exception {
		Ignite ignite = ignite(index);
		String[] path = query.getQueryPath();
		String cacheName = cacheName(ignite,path);
		String key = path[2];
		boolean rv = false;
		
		ObjectNode jsonResq = new ObjectNode(ESRelay.jsonNodeFactory);
		
		//将json转为binaryObject
		if(query.getOp().equals(ESConstants.INSERT_FRAGMENT)){
			
			IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
			BinaryObject bobj = jsonToBinaryObject(ignite,query.getQueryPath()[1],query.getQuery());
			
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
			IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
			BinaryObject bobj = jsonToBinaryObject(ignite,query.getQueryPath()[1],query.getQuery());
			
			cache.put(key, bobj);
			
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);
			jsonResq.put("_id", path[2]);
			jsonResq.put("result", "updated");
			return jsonResq;
			
		}
		else if(query.getOp().equals(ESConstants.DELETE_FRAGMENT)){
			IgniteCache<String, BinaryObject> cache = ignite(index).cache(cacheName).withKeepBinary();	
			
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
			for(Object o: list) {
				this.fLogger.fine(o.toString());
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
		IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();	
		
		String keyword = query.getParams().get("q");
		if(keyword==null){
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
		TextQuery<Object, BinaryObject> qry = new TextQuery<>(path[1], keyword);
		
		String from = query.getParams().get("from");
		String pageSize = query.getParams().get("size");
		if(pageSize!=null){
			qry.setPageSize(Integer.valueOf(pageSize)+Integer.valueOf(from));
		}
		
        QueryCursor<Cache.Entry<Object, BinaryObject>> qryCur = cache.query(qry);
        
        List<Cache.Entry<Object, BinaryObject>> list = qryCur.getAll();

        //for (Cache.Entry<String, BinaryObject> e : result)
        //    System.out.println(">>>     " + e.getValue().deserialize());
		
		
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
		String cacheName = ignite.cacheNames().iterator().next();
		IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();	
		
		SqlFieldsQuery qry = new SqlFieldsQuery(query.getSQL());
		String from = query.getParams().get("from");
		String pageSize = query.getParams().get("size");
		if(pageSize!=null){
			qry.setPageSize(Integer.valueOf(pageSize));
		}
		qry.setSchema(qry.getSchema());
		qry.setCollocated(true);
		
		
        QueryCursor<List<?>> qryCur = cache.query(qry);

        List<List<?>>  list = qryCur.getAll();
        
        CacheQueryResult res = new CacheQueryResult();
		res.setItems(list);
		
		List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
        res.setFieldsMetadata(fieldsMeta);
        
        if(query.getNamedSQL()!=null && !query.getNamedSQL().isEmpty()) {
        	// 并行执行
        	int colIndex = 0;
        	List<SubQueryCallable> tasks = new LinkedList<>();
        	for(GridQueryFieldMetadata meta: fieldsMeta) {
        		String namedSql = query.getNamedSQL().get(meta.fieldName());
        		if(namedSql!=null && namedSql.length()>1) {
        			SqlFieldsQuery subQry = new SqlFieldsQuery(namedSql);
        			for(List<?> row: list) {
        				subQry.setArgs(row.get(colIndex));
        				QueryCursor<List<?>> subQryCur = cache.query(subQry);        				
        				SubQueryCallable  task = new SubQueryCallable(subQryCur,row,colIndex);
        				tasks.add(task);        				
        			}
        		}
        		colIndex++;
        	}
        	
        	List<Future<ObjectNode>> results = ctx.pools().getExecutorService().invokeAll(tasks);
        	for(Future<ObjectNode> f: results) {
        		f.get();
        	}        	
        }
          
        return res;
	}
	
	
	protected int _addQueryResultResponses(List<ObjectNode> hits,CacheQueryResult es1Response, int limit) throws Exception {
		
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
		return 0;
		
	}
	
	@Override
	protected String mergeQueryResultResponses(Object es1Response, Object es2Response, int limit, ResponseFormat format) throws Exception {
		
		if(es1Response instanceof CacheQueryResult || es2Response instanceof CacheQueryResult) {
			CacheQueryResult result = (CacheQueryResult) es1Response;
			CacheQueryResult result2 = (CacheQueryResult) es2Response;			
			if(format==ResponseFormat.FIELDSMETA) {
				CacheQueryResult mergedResult = null;
				if(result!=null) {
					mergedResult = result;
					mergedResult.setFieldsMetadata(convertMetadata((Collection)result.getFieldsMetadata()));
				}
				if(result2!=null) {
					if(mergedResult==null) {
						mergedResult = result2;
					}
					else {
						mergedResult.setFieldsMetadata(convertMetadata((Collection)result.getFieldsMetadata()));
						mergedResult.getItems().addAll((List)result2.getItems());
					}
				}
				
				GridRestResponse resultResponse = new GridRestResponse(mergedResult);   
					
				String esResponse = ESRelay.objectMapper.writeValueAsString(resultResponse);
				return esResponse;
			}
			
			List<ObjectNode> hits = new LinkedList<>();
			int shard = _addQueryResultResponses(hits,result,limit);
			shard+=_addQueryResultResponses(hits,result2,limit);
			
			int total = 0;
			if(result!=null) total += result.getItems().size();
			if(result2!=null) total += result2.getItems().size();
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
    
    class SubQueryCallable implements Callable<ObjectNode>{
    	final int column;
    	final List<?> row;
    	final QueryCursor<List<?>> subQryCur;
    	public SubQueryCallable(QueryCursor<List<?>> subQryCur,List<?> row,int column) {
    		this.subQryCur = subQryCur;
    		this.row = row;
    		this.column = column;
    	}
    	
		@Override
		public ObjectNode call() throws Exception {
			List<List<?>> subList = subQryCur.getAll();
			if(subList.size()>0) {
			  List<?> rowOne = subList.get(0);
			  List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)subQryCur).fieldsMeta();
			  ObjectNode node = ESUtil.getObjectNode(rowOne, fieldsMeta);
			  ((List)row).set(column, node);
			  return node;
			}
			return null;
		}
    	
    }
}
