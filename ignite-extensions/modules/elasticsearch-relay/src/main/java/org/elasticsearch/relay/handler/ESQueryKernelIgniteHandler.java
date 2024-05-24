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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
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
import org.apache.ignite.plugin.PluginProvider;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.model.ESDelete;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.util.BinaryObjectMatch;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.ESUtil;
import org.elasticsearch.relay.util.HttpUtil;
import org.elasticsearch.relay.util.SqlTemplateParse;
import org.springframework.util.StringUtils;

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
			String clusterName = config.getClusterName(index+1);
			if(clusterName.isBlank()) {
				return Ignition.ignite();			
			}
			else if(clusterName.equals("default")) {
				return ctx.grid();		
			}
			else {
			   return Ignition.ignite(clusterName);	
			}
		}
		catch(IgniteIllegalStateException e) {			
			fLogger.log(Level.WARNING, "无法找到ignite集群,使用默认实例", e);			 
			return ctx.grid();
		}
	}
	
	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {				
		super.destroy();
	}
	
	/**
	 * 返回cacheName，如果是表{schema.table}，返回SQL_SCHEMA_TABLE
	 * @param path
	 * @return
	 */
	protected String cacheName(Ignite ignite,String... path){		
		String cacheName = path[0];			
		if(cacheName.indexOf('.')<=0){ // 只有一个路径片，则是cacheName
			return cacheName;
		}				
		for(String name: ignite.cacheNames()) {
			if(name.equalsIgnoreCase(cacheName)) {
				return name;
			}
		}
		String cacheName2 = "SQL_"+cacheName.toUpperCase().replace('.', '_');	
		return cacheName2;
	}
	
	/**
	 * 返回cache的typeName，如果不存在，返回defaultType
	 * @param path
	 * @return
	 */
	protected String typeName(IgniteCache<?,?> cache,String defaultType){		
		CacheConfiguration<?,?> config = cache.getConfiguration(CacheConfiguration.class);
		int pos = defaultType.lastIndexOf(".");
		String table = defaultType;	
		if(pos>0) {
			table = defaultType.substring(pos+1);
		}
		
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
		
		if(query.getAction().equals("_doc")) {
			String cacheName = cacheName(ignite,query.getIndices());
			IgniteCache<?,?> cache0 = ignite.cache(cacheName);
			if(cache0==null) {
				throw new CacheServerNotFoundException(cacheName);
			}
			
			String typeName = typeName(cache0,query.getIndices());
			String key = query.getDocId();
			
			boolean rv = false;
			
			ObjectNode jsonResq = new ObjectNode(ESRelay.jsonNodeFactory);
			
			IgniteCache<Object, BinaryObject> cache = cache0.withKeepBinary();	
			
			//将json转为binaryObject
			if(query.getOp().equals(ESConstants.INSERT_FRAGMENT)){			
				
				BinaryObject bobj = ESUtil.jsonToBinaryObject(ignite,typeName,query.getQuery());
				
				rv = cache.putIfAbsent(key, bobj);
				if(rv){
					jsonResq.put("_index", cacheName);
					jsonResq.put("_type", typeName);
					jsonResq.put("_id", key);
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
				
				jsonResq.put("_index", cacheName);
				jsonResq.put("_type", typeName);
				jsonResq.put("_id", key);
				jsonResq.put("result", "updated");
				return jsonResq;
				
			}			
			
		}
		else if(query.getAction().equals(ESConstants.BULK_FRAGMENT)){
			ObjectNode jsonResq = new ObjectNode(ESRelay.jsonNodeFactory);
			String cacheName = cacheName(ignite,query.getIndices());
			IgniteCache<?,?> cache0 = ignite.cache(cacheName);
			if(cache0==null) {
				throw new CacheServerNotFoundException(cacheName);
			}
			String typeName = typeName(cache0,query.getIndices());
			IgniteCache<Object, BinaryObject> cache = cache0.withKeepBinary();	
			
			ArrayNode list = query.getQuery().withArray(ESConstants.BULK_FRAGMENT);
			
			// TODO @byron
			for(JsonNode o: list) {
				BinaryObject bobj = ESUtil.jsonToBinaryObject(ignite,typeName,(ObjectNode)o);
				Object rowKey = o.get("id");
				cache.put(rowKey, bobj);
			}
			jsonResq.put("_index", cacheName);
			jsonResq.put("_type", typeName);
			
			jsonResq.put("result", list.size());
			return jsonResq;
		}
		
		else if(query.getAction().equals(ESConstants.INDICES_FRAGMENT)) {
			
			ObjectNode jsonResq = new ObjectNode(ESRelay.jsonNodeFactory);
			
			ObjectNode settings = query.getQuery().withObject("/settings");
			ObjectNode mappings = query.getQuery().withObject("/mappings");
			ObjectNode properties = mappings.withObject("/properties");
			String cacheName = cacheName(ignite,query.getIndices());
			IgniteCache<?,?> cache0 = ignite.cache(cacheName);
			if(cache0==null) {
				CacheConfiguration<String, BinaryObject> cfg = new CacheConfiguration<>();
		        cfg.setName(cacheName);
		        
		        if(settings.has("atomicity_mode")) {
		        	String atomicity_mode = settings.get("atomicity_mode").asText("atomic");
		        	if(atomicity_mode.equals("atomic")) {
		        		cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		        	}
		        	else {
		        		cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		        	}
		        }
		        if(settings.has("number_of_replicas")) {
		        	cfg.setBackups(settings.get("number_of_replicas").asInt(2));		        	
		        }
		        if(settings.has("data_region_name")) {
		        	cfg.setDataRegionName(settings.get("data_region_name").asText());		        	
		        }
		        
		        
		        if(properties.size()>0) {
		        	List<QueryIndex> indexes = new ArrayList<>();
		        	QueryEntity queryEntity = new QueryEntity();
		        	queryEntity.setKeyFieldName("id");
		        	queryEntity.setKeyType("java.lang.String");
		        	queryEntity.setValueType(cacheName);
		        	queryEntity.addQueryField("id", "java.lang.String", null);
		        	properties.fields().forEachRemaining((kv)->{
		        		String type = kv.getValue().get("type").asText("keyword");
		        		queryEntity.addQueryField(kv.getKey(), type, null);
		        		if(type.equals("keyword") || type.equals("text")) {
		        			indexes.add(new QueryIndex(kv.getKey(),QueryIndexType.FULLTEXT));
		        		}
		        	});
		        	queryEntity.setIndexes(indexes);
		        	cfg.setQueryEntity(queryEntity);
		        }
		        
		        cache0 = ignite.getOrCreateCache(cfg);
		        
		        jsonResq.put("result", "created");
			}
			else {
				jsonResq.put("result", "existed");
			}
			jsonResq.put("index", cacheName);
			jsonResq.put("shards_acknowledged", true);
			jsonResq.put("acknowledged", true);
			return jsonResq;
		}
		// replace spaces since they cause problems with proxies etc.
		
		query.setFormat("form");
		return super.sendEsRequest(query, index);
	}
	
	@Override
	protected Object sendEsRequest(ESQuery query, int index) throws Exception {	
		CacheQueryResult res = new CacheQueryResult();
		Ignite ignite = ignite(index);
		String path0 = query.getIndices();
		
		if(path0.isBlank()) {
			// [name, version, cluster_name, cluster_uuid, tagline]
			ArrayList<ObjectNode> items = new ArrayList<>();
			ObjectNode cluster = ESRelay.jsonNodeFactory.objectNode();
			String versionStr="{\r\n"
					+ "    \"number\" : \"7.8.0\",\r\n"
					+ "    \"build_flavor\" : \"default\",\r\n"
					+ "    \"build_type\" : \"rpm\",\r\n"
					+ "    \"build_hash\" : \"757314695644ea9a1dc2fecd26d1a43856725e65\",\r\n"
					+ "    \"build_date\" : \"2020-06-14T19:35:50.234439Z\",\r\n"
					+ "    \"build_snapshot\" : false,\r\n"
					+ "    \"lucene_version\" : \"8.5.1\",\r\n"
					+ "    \"minimum_wire_compatibility_version\" : \"6.8.0\",\r\n"
					+ "    \"minimum_index_compatibility_version\" : \"6.0.0-beta1\"\r\n"
					+ "}";
			cluster.put("name", ignite.cluster().localNode().consistentId().toString());
			cluster.set("version", ESRelay.objectMapper.readTree(versionStr));
			cluster.put("cluster_name", ignite.name());
			cluster.put("cluster_uuid", ignite.cluster().id().toString());
			cluster.put("tagline", "You Know, for Search");
			items.add(cluster);
			res.setItems(items);
			query.setResponseRootPath(null);
			query.setResponseFormat(ResponseFormat.OPERATION);
			return res;
		}
		else if(path0.equals("_nodes")) {
			query.setResponseRootPath("nodes");
			if(query.getAction().equals("plugins")) {
				ArrayList<ObjectNode> items = new ArrayList<>();
				ObjectNode plugins = ESRelay.jsonNodeFactory.objectNode();
				if(ignite.configuration().getPluginProviders()!=null) {
					for(PluginProvider<?> cfg: ignite.configuration().getPluginProviders()) {
						ObjectNode pluginInfo = ESRelay.objectMapper.convertValue(cfg, ObjectNode.class);				
						plugins.set(cfg.name(),pluginInfo);	
					}
				}
				items.add(plugins);
				res.setItems(items);
				query.setResponseFormat(ResponseFormat.OPERATION);
			}
			else {
				res.setItems(new ArrayList<>(ignite.cluster().nodes()));
				query.setResponseFormat(ResponseFormat.DATASET);
			}			
			return res;
			
		}
		else if(path0.equals("_cluster")) {
			if(query.getAction().equals("_stats")) {
				ObjectNode objectNode = ESRelay.objectMapper.convertValue(ignite.cluster().metrics(), ObjectNode.class);
				query.setResponseRootPath("cluster");
				query.setResponseFormat(ResponseFormat.OPERATION);
				return objectNode;
			}
			// get all cached
			ArrayList<ObjectNode> items = new ArrayList<>();
			ObjectNode caches = ESRelay.jsonNodeFactory.objectNode();
			for(CacheConfiguration<?,?> cacheCfg: ignite.configuration().getCacheConfiguration()) {
				ObjectNode cacheInfo = ESRelay.objectMapper.convertValue(cacheCfg, ObjectNode.class);				
				caches.set(cacheCfg.getName(),cacheInfo);				
			}
			items.add(caches);
			res.setItems(items);
			query.setResponseRootPath("index");
			query.setResponseFormat(ResponseFormat.OPERATION);
			return res;
			
		}
		
		// hava path0
		String cacheName = cacheName(ignite,path0);		
		IgniteCache<?,?> cache0 = ignite.cache(cacheName);
		if(cache0==null) {
			throw new CacheServerNotFoundException(cacheName);
		}
		
		String typeName = typeName(cache0,path0);			
		IgniteCache<?, BinaryObject> cache = cache0.withKeepBinary();	
		
		
		if(query.getAction().equals("_stats")) {			
			ObjectNode objectNode = ESRelay.objectMapper.convertValue(cache0.metrics(), ObjectNode.class);
			query.setResponseRootPath(cacheName);
			query.setResponseFormat(ResponseFormat.OPERATION);
			return objectNode;
		}
		
		if(query.getAction().equals(ESConstants.INDICES_FRAGMENT)) {
			CacheConfiguration<?,?> cfg = cache0.getConfiguration(CacheConfiguration.class);
			ObjectNode objectNode = ESRelay.objectMapper.convertValue(cfg, ObjectNode.class);
			query.setResponseRootPath(cacheName);
			query.setResponseFormat(ResponseFormat.OPERATION);
			return objectNode;			
		}	
		
		else if(query.getAction().equals(ESConstants.SEARCH_FRAGMENT)) {
			ObjectNode matchQurey = ESRelay.jsonNodeFactory.objectNode();
			
			String[] keywords = query.getParams().get("q");
			String keyword = "";
			if(keywords==null){
				StringBuilder keywordBuilder = new StringBuilder();
				ObjectNode queryObj = query.getQuery().withObject("/query");
				if(queryObj.has("multi_match")){
					ObjectNode multi_match = queryObj.withObject("/multi_match");
					ArrayNode fields = multi_match.withArray("/fields");
					String queryString = multi_match.get("query").asText();
					
					for(int i=0;i<fields.size();i++){
						keywordBuilder.append(fields.get(i)+":"+queryString+" ");
					}
				}
				else if(queryObj.has("match")){
					ObjectNode match = queryObj.withObject("/match");
					match.fields().forEachRemaining(kv->{
						String field = kv.getKey();
						String queryString = kv.getValue().get("query").asText();
						String op = kv.getValue().get("operator").asText();
						keywordBuilder.append(field+":"+queryString+" ");
					});				
					
				}
				else if(queryObj.has("terms")){
					ObjectNode match = queryObj.withObject("/terms");
					match.fields().forEachRemaining(kv->{
						String field = kv.getKey();						
						matchQurey.set(field, kv.getValue());
					});				
					
				}
				keyword = keywordBuilder.toString();
			}
			else {
				keyword = String.join(" ", keywords);
			}
			int iFrom = query.getFrom();
			int iMaxSize = query.getLimit();
			BinaryObjectMatch match = matchQurey.isEmpty()? null: new BinaryObjectMatch(matchQurey);
			
			Query<Cache.Entry<Object, BinaryObject>> qry;
			if(StringUtils.hasText(keyword)) {
				TextQuery<Object, BinaryObject> textQry = new TextQuery<>(typeName, keyword);
				textQry.setFitler(match);							
				textQry.setLimit(iFrom+iMaxSize);
				qry = textQry;
			}
			else {
				ScanQuery<Object, BinaryObject> scanfQry = new ScanQuery<>(match);
				qry = scanfQry;
			}
			
			
			for(Map.Entry<String, String[]> param: query.getParams().entrySet()) {
				if(param.getKey().equals("pageSize")) {
					String[] pageSize = query.getParams().get("pageSize");
					if(pageSize!=null){
						qry.setPageSize(Integer.valueOf(pageSize[0]));
					}
				}				
			}			
			
			
	        QueryCursor<Cache.Entry<Object, BinaryObject>> qryCur = cache.query(qry);
	        
	        List<Cache.Entry<Object, BinaryObject>> all = new ArrayList<>();

	        try {
	            Iterator<Cache.Entry<Object, BinaryObject>> iter = qryCur.iterator(); // Implicitly calls iterator() to do all checks.
	            int i=0;
	            while (iter.hasNext() && all.size()<=iMaxSize+iFrom) {	            	
	            	all.add(iter.next());
	            	i++;
	            }
	            
	            res.setItems(all);
				List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
		        res.setFieldsMetadata(convertMetadata(fieldsMeta));
	        }
	        finally {
	        	qryCur.close();
	        }
		}
		else if(query.getAction().equals(ESConstants.ALL_FRAGMENT)) {			
			ObjectNode matchQurey = ESRelay.jsonNodeFactory.objectNode();
			
			matchQurey = query.getQuery().withObject("/query");
			
			BinaryObjectMatch match = matchQurey.isEmpty()? null: new BinaryObjectMatch(matchQurey);
			
			ScanQuery<Object, BinaryObject> qry = new ScanQuery<>(match);
			int iFrom = query.getFrom();
			int iMaxSize = query.getLimit();
			for(Map.Entry<String, String[]> param: query.getParams().entrySet()) {
				if(param.getKey().equals("pageSize")) {
					String[] pageSize = query.getParams().get("pageSize");
					if(pageSize!=null){
						qry.setPageSize(Integer.valueOf(pageSize[0]));
					}
				}				
			}
			
	        QueryCursor<Cache.Entry<Object, BinaryObject>> qryCur = cache.query(qry);	        
	        
	        List<Cache.Entry<Object, BinaryObject>> all = new ArrayList<>();

	        try {
	            Iterator<Cache.Entry<Object, BinaryObject>> iter = qryCur.iterator(); // Implicitly calls iterator() to do all checks.
	            int i=0;
	            while (iter.hasNext() && all.size()<=iMaxSize+iFrom) {	            	
	            	all.add(iter.next());
	            	i++;
	            }
	            
	            res.setItems(all);
				List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();
		        res.setFieldsMetadata(convertMetadata(fieldsMeta));
	        }
	        finally {
	        	qryCur.close();
	        }			
			
		}		

        return res;
		
	}
	
	@Override
	protected Object sendEsRequest(ESDelete query, int index) throws Exception {			
		Ignite ignite = ignite(index);
		String cacheName = cacheName(ignite,query.getIndices());		
		IgniteCache<String,?> cache0 = ignite.cache(cacheName);
		if(cache0==null) {
			throw new CacheServerNotFoundException(cacheName);
		}
		
		GridRestResponse res = new GridRestResponse();
		
		if(query.getDocId()==null && !query.getIndices().isBlank() && query.getAction().equals(ESConstants.INDICES_FRAGMENT)) {
			cache0.destroy();
			res.setResponse(true);
		}
		else if(query.getAction().equals("_doc")) {		// {index}/_doc/{id}	
			
			String id = query.getDocId();
			if(id!=null) {
				res.setResponse(cache0.remove(id));
			}
			else {
				cache0.clear();
				res.setResponse(true);
			}
		}		

        return res;
		
	}
	
	@Override
	protected Object sendEsRequest(ESViewQuery query, int index) throws Exception {
		Ignite ignite = ignite(index);
		
		IgniteEx igniteEx = (IgniteEx) ignite;
		
		
		SqlFieldsQuery qry = new SqlFieldsQuery(query.buildSQL());
		qry.setSchema(query.getSchema());
		qry.setPageSize(query.getLimit());
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
			Iterator<Object> es1Hits = (Iterator)result.getItems().iterator();
			
			List<GridQueryFieldMetadata> fieldsMeta = (List)result.getFieldsMetadata();			
			
			while (es1Hits.hasNext() && hits.size() < limit) {
				Object item = es1Hits.next();
				if(fieldsMeta.size()>0 && item instanceof List) {
					ObjectNode node = ESUtil.getObjectNode((List)item, fieldsMeta);
					if (node!=null) {					
						addHit(hits, node);
					}
				}
				else {
					addHit(hits, makeObjectNode(item));
				}
			}
			return result.getItems().size()>0? 1: 0;
		}
		if(es1Response instanceof List) {
			List<Object> result = (List) es1Response;			

			// mix results 50:50 as far as possible
			Iterator<Object> es1Hits = result.iterator();
			
			while (es1Hits.hasNext() && hits.size() < limit) {
				Object node = es1Hits.next();
				if (node!=null) {					
					addHit(hits, makeObjectNode(node));
				}				
			}
			return result.size()>0? 1: 0;
		}
		return 0;
		
	}
	
	@Override
	protected String mergeQueryResultResponses(List<Object> esResponses, ESQuery query) throws Exception {
		List<ObjectNode> hits = new LinkedList<ObjectNode>();		
		int totalHits = 0;
		int shards = 0;
		int limit = query.getLimit();
		int offset = query.getFrom();
		String rootPath = query.getResponseRootPath();
		ResponseFormat format = query.getResponseFormat();
		
		CacheQueryResult mergedResult = null;
		for(int i=0;i<esResponses.size();i++){
			Object es1Response = esResponses.get(i);
			if(es1Response instanceof CacheQueryResult) {
				CacheQueryResult result = (CacheQueryResult) es1Response;				
				if(mergedResult==null) {
					mergedResult = result;
					mergedResult.setFieldsMetadata(convertMetadata((Collection)result.getFieldsMetadata()));
				}
				else {					
					mergedResult.getItems().addAll((Collection)result.getItems());
				}
				totalHits += result.getItems().size();
				shards+=1;		
			}
			else if(es1Response instanceof List) {
				List<ObjectNode> result = (List) es1Response;
				
				shards += _addQueryResultResponses(hits,result,limit);
				totalHits += result.size();			
			}
			else if(es1Response instanceof ObjectNode) {
				ObjectNode es1Json=(ObjectNode)es1Response;
				if (es1Json.has(ESConstants.R_ERROR)) {
					continue;
				}
				addHit(hits, es1Json);
				shards+=1;
				totalHits +=1;
			}				
			else {
				return super.mergeQueryResultResponses(esResponses, query);
			}
		}
		
		if(format==ResponseFormat.FIELDSMETA) {
			if(mergedResult==null) {
				throw new IllegalArgumentException("ES 1.x error: mergedResult is null" + CacheQueryResult.class);
			}
			return ESRelay.objectMapper.writer().writeValueAsString(mergedResult);
		}
		else if(format==ResponseFormat.OPERATION) {
			Object first = null;
			if(mergedResult==null) {
				first = hits.get(0);
			}
			else {
				first = mergedResult.getItems().iterator().next();
			}
			if(rootPath!=null) {
				ObjectNode res = ESRelay.jsonNodeFactory.objectNode();
				res.set(rootPath, ESRelay.objectMapper.convertValue(first,ObjectNode.class));
				
				return ESRelay.objectMapper.writer().writeValueAsString(res);
			}
			else {
				return ESRelay.objectMapper.writer().writeValueAsString(first);
			}
		}
		
		if(mergedResult!=null) {
			_addQueryResultResponses(hits,mergedResult,limit);
		}
		
		if(hits.size() > offset+limit) {
			hits = hits.subList(offset, offset+limit);
		}
		else {
			hits = hits.subList(offset,hits.size());
		}
		
		// add up data
		ESResponse mergedResponse = new ESResponse(hits);
		mergedResponse.setShards(shards);
		mergedResponse.setTotalHits(totalHits);
		
		if(format==ResponseFormat.DATASET) {
			return mergedResponse.toDataset(rootPath).toPrettyString();
		}
		else if(format==ResponseFormat.HITS) {
			return mergedResponse.toJSON().toPrettyString();	
		}
		else {
			throw new IllegalArgumentException("ES 1.x error: Not support format " + format);
		}
	}

	 /**
     * @param meta Internal query field metadata.
     * @return Rest query field metadata.
     */
    private Collection<CacheQueryFieldsMetaResult> convertMetadata(Collection<?> meta) {
        List<CacheQueryFieldsMetaResult> res = new ArrayList<>();

        if (meta != null) {
            for (Object info : meta) {
            	if(info instanceof GridQueryFieldMetadata) {
            		res.add(new CacheQueryFieldsMetaResult((GridQueryFieldMetadata)info));
            	}
            	else if(info instanceof String) {
            		CacheQueryFieldsMetaResult metaData = new CacheQueryFieldsMetaResult();
            		metaData.setFieldName(info.toString());
            		res.add(metaData);
            	}
            }
                
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
