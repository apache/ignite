package org.elasticsearch.relay.handler;

import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.HttpUtil;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Central query handler splitting up queries between multiple ES instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryKernelIgniteHandler extends ESQueryHandler{
	Ignite ignite;	
	GridKernalContext ctx;
	
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
	}
	
	protected String cacheName(String... path){		
		String schema = path[0];
		String table = path[1];		
		if(table.length()==0){
			return schema;
		}			
		String cacheName = "SQL_"+schema+"_"+table.toUpperCase();		
		return cacheName;
	}
	
	protected BinaryObject jsonToBinaryObject(String typeName,JSONObject obj){	
		BinaryObjectBuilder bb = ignite.binary().builder(typeName);
		Iterator it = obj.keys();
	    while(it.hasNext()){
	    	String $key =  it.next().toString();
	    	Object $value ;
			try {
				$value = obj.get($key);
				
				bb.setField($key, $value);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    return bb.build();
	}

	protected String sendEsRequest(ESUpdate query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getQueryUrl();
		String[] path = query.getQueryPath();
		String cacheName = cacheName(path);
		String key = path[2];
		boolean rv = false;
		
		JSONObject jsonResq = new JSONObject();
		
		//将json转为binaryObject
		if(query.getOp().equals(ESConstants.INSERT_FRAGMENT)){
			IgniteCache<String, BinaryObject> cache = ignite.getOrCreateCache(cacheName).withKeepBinary();
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
			JSONArray list = query.getQuery().getJSONArray(ESConstants.BULK_FRAGMENT);
			jsonResq.put("_index", path[0]);
			jsonResq.put("_type", path[1]);			
			jsonResq.put("result", list.length());
			return jsonResq.toString();
		}

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = null;
		query.setFormat("form");
		return super.sendEsRequest(query, esUrl);
	}
	
	protected String sendEsRequest(ESQuery query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getQueryUrl();		
				
		String[] path = query.getQueryPath();
		String cacheName = cacheName(path);
		IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();	
		
		TextQuery<String, BinaryObject> qry = new TextQuery<>(path[1], "TX");
		
        QueryCursor<Cache.Entry<String, BinaryObject>> employees = cache.query(qry);

        System.out.println();
        System.out.println(">>> Employees living in Texas:");

        for (Cache.Entry<String, BinaryObject> e : employees.getAll())
            System.out.println(">>>     " + e.getValue().deserialize());

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = null;
		query.setFormat("form");
		
		return super.sendEsRequest(query, esUrl);
	}
	
	
	protected JSONObject mergeResponses(ESQuery query, String es1Response, String es2Response) throws Exception {
		ESResponse es1Resp = new ESResponse();
		ESResponse es2Resp = new ESResponse();

		// TODO: recognize non-result responses and only use valid responses?
		if (es1Response != null) {
			JSONObject es1Json = new JSONObject(es1Response);

			if (!es1Json.has(ESConstants.R_ERROR)) {
				es1Resp = new ESResponse(es1Json);
			} else {
				throw new Exception("ES 1.x error: " + es1Response);
			}
		}
		if (es2Response != null) {
			JSONObject es2Json = new JSONObject(es2Response);

			if (!es2Json.has(ESConstants.R_ERROR)) {
				es2Resp = new ESResponse(es2Json);
			} else {
				throw new Exception("ES 2.x error: " + es2Response);
			}
		}

		List<JSONObject> hits = new LinkedList<JSONObject>();

		// mix results 50:50 as far as possible
		Iterator<JSONObject> es1Hits = es1Resp.getHits().iterator();
		Iterator<JSONObject> es2Hits = es2Resp.getHits().iterator();

		// limit returned amount if size is specified
		final int limit = getLimit(query);

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

		return mergedResponse.toJSON();
	}

	protected void addHit(List<JSONObject> hits, JSONObject hit) throws Exception {
		// retrieve type and handle postprocessing
		String type = hit.getString(ESConstants.R_HIT_TYPE);

		IPostProcessor pp = fPostProcs.get(type);
		if (pp != null) {
			hit = pp.process(hit);
		}

		// postprocessors active for all types
		for (IPostProcessor gpp : fGlobalPostProcs) {
			hit = gpp.process(hit);
		}

		hits.add(hit);
	}

}
