package org.elasticsearch.relay.handler;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
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
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryResult;
import org.apache.ignite.plugin.PluginProvider;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.util.BinaryObjectMatch;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.ESUtil;
import org.elasticsearch.relay.util.HttpUtil;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Central query handler splitting up queries between multiple ignite instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryClientIgniteHandler extends ESQueryHandler{
	
	
	IgniteClient[] igniteClient;
	
	public ESQueryClientIgniteHandler(ESRelayConfig config) throws Exception{
		super(config);		
		int size = config.getClusterSize();
		igniteClient = new IgniteClient[size];
		
		//thin
		for(int i=1;i<size+1;i++) {
			String host = config.getElasticApiHost(i);	
			int port =  config.getElasticApiPort(i);
			if(host!=null && !host.isEmpty()){
				
				ClientConfiguration cfg = new ClientConfiguration().setAddresses(host+":"+port);	
		        try {
		        	IgniteClient igniteClient = Ignition.startClient(cfg);
		        			
		            System.out.println();
		            System.out.println(">>> Thin client ignite started.");          
		            this.igniteClient[i-1] = igniteClient;
		        }
		        catch (ClientException e) {
		            System.err.println(e.getMessage());
		            fLogger.log(Level.SEVERE, e.getMessage(), e);
		        }
		        catch (Exception e) {
		            System.err.format("Unexpected failure: %s\n", e);
		            fLogger.log(Level.SEVERE, e.getMessage(), e);
		        }
		        
			}
		}	
		
	}
	
	/**
	 * 返回cacheName，如果是表{schema.table}，返回SQL_SCHEMA_TABLE
	 * @param path
	 * @return
	 */
	protected String cacheName(IgniteClient ignite,String index){		
		String cacheName = index;	
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
	
	protected ClientCache<Object, BinaryObject> cache(IgniteClient ignite,String index){		
		String cacheName = cacheName(ignite,index);
		ClientCache<?, ?> cache0 = ignite.cache(cacheName);
		if(cache0==null) {
			throw new CacheServerNotFoundException(cacheName);
		}		
		ClientCache<Object, BinaryObject> cache = cache0.withKeepBinary();
		return cache;
	}
	

	/**
	 * 返回cache的typeName，如果不存在，返回defaultType
	 * @param path
	 * @return
	 */
	protected String typeName(ClientCache<?,?> cache,String defaultType){		
		ClientCacheConfiguration config = cache.getConfiguration();
		int pos = defaultType.lastIndexOf(".");
		String table = defaultType;	
		if(pos>0) {
			table = defaultType.substring(pos+1);
		}
		
		if(config.getQueryEntities().length==1){
			return config.getQueryEntities()[0].getValueType();
		}				
		if(config.getQueryEntities().length==0){
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
	protected Object sendEsRequest(ESQuery query, int index) throws Exception {	
		CacheQueryResult res = new CacheQueryResult();
		IgniteClient ignite = igniteClient[index];
		String path0 = query.getIndices();
		
		if(path0.isBlank()) {
			if(query.getAction().equals(ESConstants.ALL_FRAGMENT)) {	
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
				cluster.put("name", ignite.cluster().node().consistentId().toString());
				cluster.set("version", ESRelay.objectMapper.readTree(versionStr));
				cluster.put("cluster_name", ignite.cluster().node().consistentId().toString());
				cluster.put("cluster_uuid", ignite.cluster().node().id().toString());
				cluster.put("tagline", "You Know, for Search");
				items.add(cluster);
				res.setItems(items);
				query.setResponseRootPath(null);
				query.setResponseFormat(ResponseFormat.OPERATION);
				return res;
			}
			else if(query.getAction().equals(ESConstants.GET_FRAGMENT)) {
				
				ArrayNode docs = query.getQuery().withArray("/docs");
				ArrayList<ObjectNode> items = new ArrayList<>();				
				docs.forEach(e->{
					if(!e.isNull() && e.has("_id")) {
						ObjectNode hit = ESRelay.jsonNodeFactory.objectNode();
						
						Object _key = ESUtil.jsonNodeToObject(e.get("_id"));
						String _cache = e.get("_index").asText();
						ClientCache<Object, BinaryObject> cache = cache(ignite,_cache);
						String cacheName = cache.getName();
						BinaryObject row = cache.get(_key);
						if(row!=null) {
							hit.put("_index", cacheName);
							hit.put("_type", "_doc");
							hit.set("_id", e.get("_id"));
							hit.put("found", true);
							ObjectNode doc = ESRelay.objectMapper.convertValue(row,ObjectNode.class);
							hit.set("_source",doc);
							
							items.add(hit);
						}
					}
				});
				
		        res.setItems(items);
		        query.setResponseRootPath("docs");
		        return res;
				
			}
		}
		else if(path0.equals("_nodes")) {
			query.setResponseRootPath("nodes");
			if(query.getAction().equals("plugins")) {
				ArrayList<ObjectNode> items = new ArrayList<>();
				ObjectNode plugins = ESRelay.jsonNodeFactory.objectNode();
				// todo add plugins {name:plugin_info}
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
				ObjectNode objectNode = ESRelay.objectMapper.convertValue(ignite.cluster().node().metrics(), ObjectNode.class);			
				query.setResponseRootPath("cluster");
				query.setResponseFormat(ResponseFormat.OPERATION);
				return objectNode;
			}
			// get all cached
			ArrayList<ObjectNode> items = new ArrayList<>();
			ObjectNode caches = ESRelay.jsonNodeFactory.objectNode();
			for(String cacheName: ignite.cacheNames()) {
				ClientCache<?, ?> cache = ignite.cache(cacheName);
				ObjectNode cacheInfo = ESRelay.objectMapper.convertValue(cache.getConfiguration(), ObjectNode.class);				
				caches.set(cacheName,cacheInfo);				
			}
			items.add(caches);
			res.setItems(items);
			query.setResponseRootPath("index");
			query.setResponseFormat(ResponseFormat.OPERATION);
			return res;
			
		}
		
		// hava path0
		ClientCache<Object, BinaryObject> cache = cache(ignite,query.getIndices());
		String cacheName = cache.getName();
		String typeName = typeName(cache,query.getIndices());
		
		
		if(query.getAction().equals("_stats")) {			
			ObjectNode objectNode = ESRelay.objectMapper.convertValue(ignite.cluster().node().metrics(), ObjectNode.class);
			query.setResponseRootPath(cacheName);
			query.setResponseFormat(ResponseFormat.OPERATION);
			return objectNode;
		}
		
		if(query.getAction().equals(ESConstants.INDICES_FRAGMENT)) {
			ClientCacheConfiguration cfg = cache.getConfiguration();
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
		        res.setFieldsMetadata(ESUtil.convertMetadata(fieldsMeta));
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
		        res.setFieldsMetadata(ESUtil.convertMetadata(fieldsMeta));
	        }
	        finally {
	        	qryCur.close();
	        }			
			
		}
		else if(query.getAction().equals(ESConstants.GET_FRAGMENT)) {	
			
			
			ArrayNode ids = query.getQuery().withArray("/id");
			
			Set<Object> keys = new HashSet<>();
			ids.forEach(e->{
				if(!e.isNull()) {
					keys.add(ESUtil.jsonNodeToObject(e));
				}
			});			
			
			
	        Map<Object, BinaryObject> qryResult = cache.getAll(keys);	        
	        
	        res.setItems(qryResult.entrySet());			
			
		}

        return res;
		
	}
	
	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {
		super.destroy();
		
		for(IgniteClient client: igniteClient){
			try {
				if(client!=null) {
					client.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				fLogger.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
	}	

}
