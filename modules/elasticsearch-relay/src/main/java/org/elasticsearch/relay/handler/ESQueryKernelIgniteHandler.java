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
import java.util.logging.Level;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryFieldsMetaResult;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryResult;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;


/**
 * Central query handler splitting up queries between multiple ES instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryKernelIgniteHandler extends ESQueryHandler{
	protected Ignite ignite;	
	protected GridKernalContext ctx;
	
	
	public ESQueryKernelIgniteHandler(ESRelayConfig config,GridKernalContext ctx) throws Exception{
		super(config);
		this.ctx = ctx;
		//then
		if(ctx!=null){

			ignite = ctx.grid();
		}
		
	}
	
	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {				
		super.destroy();
		ignite = null;
	}
	
	/**
	 * 返回cacheName，如果是表，返回SQL_SCHEMA_TABLE
	 * @param path
	 * @return
	 */
	protected String cacheName(String... path){		
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
	
	protected BinaryObject jsonToBinaryObject(String typeName,ObjectNode obj){	
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
	protected String sendEsRequest(ESUpdate query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getQueryUrl();
		String[] path = query.getQueryPath();
		String cacheName = cacheName(path);
		String key = path[2];
		boolean rv = false;
		
		ObjectNode jsonResq = new ObjectNode(ESRelay.jsonNodeFactory);
		
		//将json转为binaryObject
		if(query.getOp().equals(ESConstants.INSERT_FRAGMENT)){
			IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
			BinaryObject bobj = jsonToBinaryObject(query.getQueryPath()[1],query.getQuery());
			
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
			return jsonResq.toString();
		}
		else if(query.getOp().equals(ESConstants.UPDATE_FRAGMENT)){
			IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
			BinaryObject bobj = jsonToBinaryObject(query.getQueryPath()[1],query.getQuery());
			
			cache.put(key, bobj);
			
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);
			jsonResq.put("_id", path[2]);
			jsonResq.put("result", "updated");
			return jsonResq.toString();
			
		}
		else if(query.getOp().equals(ESConstants.DELETE_FRAGMENT)){
			IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();	
			
			cache.remove(key);
			
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);
			jsonResq.put("_id", path[2]);
			jsonResq.put("result", "deleted");
			return jsonResq.toString();
			
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
			return jsonResq.toString();
		}

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = null;
		query.setFormat("form");
		return super.sendEsRequest(query, esUrl);
	}
	
	@Override
	protected String sendEsRequest(ESQuery query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getQueryUrl();		
				
		String[] path = query.getQueryPath();
		String cacheName = cacheName(path);
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

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");
		
		CacheQueryResult res = new CacheQueryResult();
		res.setItems(list);
		
		List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
        res.setFieldsMetadata(convertMetadata(fieldsMeta));

		
		GridRestResponse result = new GridRestResponse(res);   
		String es1Response = null;
		query.setFormat("form");
		es1Response = ESRelay.objectMapper.writeValueAsString(result);
		return es1Response;
		//return super.sendEsRequest(query, esUrl);
	}
	
	@Override
	protected String sendEsRequest(ESViewQuery query, String esUrl) throws Exception {
		String[] path = query.getQueryPath();
		String cacheName = cacheName(path);
		IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();	
		
		SqlFieldsQuery qry = new SqlFieldsQuery(query.getSQL());
		String from = query.getParams().get("from");
		String pageSize = query.getParams().get("size");
		if(pageSize!=null){
			qry.setPageSize(Integer.valueOf(pageSize)+Integer.valueOf(from));
		}
		
        QueryCursor<List<?>> qryCur = cache.query(qry);

        List<?>  list = qryCur.getAll();
        
        CacheQueryResult res = new CacheQueryResult();
		res.setItems(list);
		
		List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
        res.setFieldsMetadata(convertMetadata(fieldsMeta));


        GridRestResponse result = new GridRestResponse(res);        
       
		String es1Response = null;
		query.setFormat("form");		
		es1Response = ESRelay.objectMapper.writeValueAsString(result);
		return es1Response;
		//return super.sendEsRequest(query, esUrl);
	}
	
	@Override
	protected String mergeResponses(String es1Response, String es2Response,int limit) throws Exception {
		
		if(es2Response==null){
			return es1Response;
		}

		if(es1Response==null){
			return es2Response;
		}		
		
		ESResponse es1Resp = new ESResponse();
		ESResponse es2Resp = new ESResponse();

		// TODO: recognize non-result responses and only use valid responses?
		if (es1Response != null) {
			ObjectNode es1Json = (ObjectNode)ESRelay.objectMapper.readTree(es1Response);

			if (!es1Json.has(ESConstants.R_ERROR)) {
				es1Resp = new ESResponse(es1Json);
			} else {
				throw new Exception("ES 1.x error: " + es1Response);
			}
		}
		if (es2Response != null) {
			ObjectNode es2Json = (ObjectNode)ESRelay.objectMapper.readTree(es2Response);

			if (!es2Json.has(ESConstants.R_ERROR)) {
				es2Resp = new ESResponse(es2Json);
			} else {
				throw new Exception("ES 2.x error: " + es2Response);
			}
		}

		List<ObjectNode> hits = new LinkedList<ObjectNode>();

		// mix results 50:50 as far as possible
		Iterator<ObjectNode> es1Hits = es1Resp.getHits().iterator();
		Iterator<ObjectNode> es2Hits = es2Resp.getHits().iterator();

		while ((es1Hits.hasNext() || es2Hits.hasNext()) && hits.size() < limit) {
			if (es1Hits.hasNext()) {
				addHit(hits, es1Hits.next());
			}
			if (es2Hits.hasNext()) {
				addHit(hits, es2Hits.next());
			}
		}

		// add up data
		ESResponse mergedResponse = new ESResponse(hits);
		mergedResponse.setShards(es1Resp.getShards() + es2Resp.getShards());
		mergedResponse.setTotalHits(es1Resp.getTotalHits() + es2Resp.getTotalHits());

		return mergedResponse.toJSON().toPrettyString();
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
}
