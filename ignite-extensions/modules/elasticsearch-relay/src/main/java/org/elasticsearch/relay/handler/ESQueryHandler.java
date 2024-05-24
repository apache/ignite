package org.elasticsearch.relay.handler;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.cache.Cache;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.client.IgniteClient;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.filters.BlacklistFilter;
import org.elasticsearch.relay.filters.IFilter;
import org.elasticsearch.relay.filters.LiferayFilter;
import org.elasticsearch.relay.filters.NuxeoFilter;
import org.elasticsearch.relay.model.ESDelete;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.model.ESViewQuery;
import org.elasticsearch.relay.permissions.IPermCrawler;
import org.elasticsearch.relay.permissions.LiferayCrawler;
import org.elasticsearch.relay.permissions.NuxeoCrawler;
import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.postprocess.ContentPostProcessor;
import org.elasticsearch.relay.postprocess.HtmlPostProcessor;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.postprocess.LiferayPostProcessor;
import org.elasticsearch.relay.postprocess.MailPostProcessor;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.ESUtil;
import org.elasticsearch.relay.util.HttpUtil;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;



/**
 * Central query handler splitting up queries between multiple ES instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryHandler {
	
	final PermissionCrawler fPermCrawler;	
	
	
	protected final String[] fEsUrl;
	protected final Set<String>[] fEsIndices;	

	protected final Map<String, IFilter> fIndexFilters, fTypeFilters;

	//对搜索结果进行处理
	protected final MultiValueMap<String, IPostProcessor> fPostProcs;
	protected final List<IPostProcessor> fGlobalPostProcs;

	protected final BlacklistFilter[] fEsBlacklistFilter;	

	protected final Logger fLogger;

	protected final boolean fLogRequests;
	
	protected ESRelayConfig config;

	/**
	 * Creates a new query handler using the given configuration, initializing
	 * all filters and post processors and starting the permission crawler
	 * thread.
	 * 
	 * @param config
	 *            configuration object to use
	 * @throws Exception
	 *             if initialization fails
	 */
	public ESQueryHandler(ESRelayConfig config) throws Exception {
		this.config = config;
		fLogRequests = config.getLogRequests();
		
		int size = config.getClusterSize();		

		fEsUrl = new String[size];	

		fEsIndices = new Set[size];		
		
		fEsBlacklistFilter = new BlacklistFilter[size];
		
		// http
		for(int i=1;i<size+1;i++) {
			
			fEsUrl[i-1] = config.getElasticUrl(i);
			fEsIndices[i-1] = config.getElasticIndices(i);
			fEsBlacklistFilter[i-1] = new BlacklistFilter(config.getEsBlacklistIndices(i), config.getEsBlacklistTypes(i));
		}
		
		fLogger = Logger.getLogger(this.getClass().getName());

		// initialize permission crawlers
		List<IPermCrawler> crawlers = new ArrayList<IPermCrawler>();

		crawlers.add(new NuxeoCrawler(config.getNuxeoUrl(), config.getNuxeoUser(), config.getNuxeoPassword()));

		crawlers.add(new LiferayCrawler(config.getLiferayUrl(), config.getLiferayCompanyId(), config.getLiferayUser(), config.getLiferayPassword()));

		String permGetUrl = config.getPermissionsCrawlUrl();
		fPermCrawler = new PermissionCrawler(permGetUrl, crawlers, config.getPermCrawlInterval());
		
		if(config.getPermCrawlInterval()>= 0){
			
			ESRelay.scheduleExecutorService.scheduleWithFixedDelay(fPermCrawler, config.getPermCrawlInterval(), config.getPermCrawlInterval(), TimeUnit.MILLISECONDS);    
		}

		fIndexFilters = new HashMap<String, IFilter>();
		fTypeFilters = new HashMap<String, IFilter>();
				

		LiferayFilter lrFilter = new LiferayFilter(config.getLiferayTypes(), config.getLiferayPassthroughRoles());
		fIndexFilters.put(config.getLiferayIndex(), lrFilter);
		for (String type : config.getLiferayTypes()) {
			fTypeFilters.put(type, lrFilter);
		}

		NuxeoFilter nxFilter = new NuxeoFilter(config.getNuxeoTypes());
		fIndexFilters.put(config.getNuxeoIndex(), nxFilter);
		for (String type : config.getNuxeoTypes()) {
			fTypeFilters.put(type, nxFilter);
		}

		// initialize and register post processors
		fPostProcs = new LinkedMultiValueMap<String, IPostProcessor>();
		
		// add shortening post processor for all types
		fGlobalPostProcs = new ArrayList<>();
		
		Map<String, IPostProcessor> allPostProcs = ESRelay.context.getBeansOfType(IPostProcessor.class);
		for(IPostProcessor pp: allPostProcs.values()) {
			if(pp.getTypeSet()==null) {
				fGlobalPostProcs.add(pp);
			}
			else {
				for (String type : pp.getTypeSet()) {
					fPostProcs.add(type, pp);
				}
			}
		}

		
	}
	

	/**
	 * Handles and processes a update cmd and its results, returning the merged
	 * results from multiple ES instances.
	 * 
	 * @param query
	 *            query sent by a user
	 * @param user
	 *            ID of the user that sent a query
	 * @return result or error object
	 * @throws Exception
	 *             if an internal error occurs
	 */
	public String handleRequest(ESUpdate query, String user) throws Exception {
		IntStream s = IntStream.range(0, fEsIndices.length);
		if(fEsIndices.length>=4) {
			s = s.parallel();
		}
		
		List<Object> result = s.mapToObj(i->{
			// url index and type parameters and in-query parameters
			ESUpdate es1Query = getInstanceUpdate(query, fEsIndices[i]);
			Object es1Response = null;

			// process requests and run through filters
			// forward request to Elasticsearch instances
			// don't send empty queries
			if (!es1Query.isCancelled()) {
				
				try {
					es1Response = sendEsRequest(es1Query, i);
				} catch (Exception e) {
					es1Response = makeObjectNode(e, 404);
				}
			}
			
        	return es1Response;
        	
        }).filter(r->!Objects.isNull(r)).collect(Collectors.toList()); 
		
		
		
		// merge results
		// limit returned amount if size is specified
		
		String response = mergeOperationResultResponses(result, false, query.getResponseFormat());
		
		return response;
		
	}

	/**
	 * Handles and processes a query and its results, returning the merged
	 * results from multiple ES instances.
	 * 
	 * @param query
	 *            query sent by a user
	 * @param user
	 *            ID of the user that sent a query
	 * @return result or error object
	 * @throws Exception
	 *             if an internal error occurs
	 */
	public String handleRequest(ESQuery query, String user) throws Exception {
		IntStream s = IntStream.range(0, fEsIndices.length);
		if(fEsIndices.length>=4) {
			s = s.parallel();
		}
		
		List<Object> result = s.mapToObj(i->{
			// url index and type parameters and in-query parameters
			ESQuery es1Query = getInstanceQuery(query, fEsIndices[i]);
			Object es1Response = null;
			
			es1Query = handleFiltering(user, es1Query, fEsBlacklistFilter[i]);

			// process requests and run through filters
			// forward request to Elasticsearch instances
			// don't send empty queries
			if (!es1Query.isCancelled()) {
				
				try {
					es1Response = sendEsRequest(es1Query, i);
					query.setResponseFormat(es1Query.getResponseFormat());
					query.setResponseRootPath(es1Query.getResponseRootPath());
				} catch (Exception e) {
					es1Response = makeObjectNode(e, 404);
				}
			}
			
        	return es1Response;
        	
        }).filter(r->!Objects.isNull(r)).collect(Collectors.toList());
		
		
		// merge results
		// limit returned amount if size is specified		
		String response = mergeQueryResultResponses(result, query);
		
		return response;
	}
	
	/**
	 * Handles and processes a query and its results, returning the merged
	 * results from multiple ES instances.
	 * 
	 * @param query
	 *            query sent by a user
	 * @param user
	 *            ID of the user that sent a query
	 * @return result or error object
	 * @throws Exception
	 *             if an internal error occurs
	 */
	public String handleRequest(ESDelete query, String user) throws Exception {
		
		IntStream s = IntStream.range(0, fEsIndices.length);
		if(fEsIndices.length>=4) {
			s = s.parallel();
		}
		
		List<Object> result = s.mapToObj(i->{
			// url index and type parameters and in-query parameters
			ESDelete es1Query = getInstanceQuery(query, fEsIndices[i]);
			Object es1Response = null;

			// process requests and run through filters
			// forward request to Elasticsearch instances
			// don't send empty queries
			if (!es1Query.isCancelled()) {
				
				try {
					es1Response = sendEsRequest(es1Query, i);
				} catch (Exception e) {
					es1Response = makeObjectNode(e,404);
				}
			}
			
        	return es1Response;
        	
        }).filter(r->!Objects.isNull(r)).collect(Collectors.toList()); 
		
		
		
		// merge results
		// limit returned amount if size is specified
		
		String response = mergeOperationResultResponses(result, false, query.getResponseFormat());
		
		return response;
	}
	
	public String handleRequest(ESViewQuery query, String user) throws Exception {
		IntStream s = IntStream.range(0, fEsIndices.length);
		if(fEsIndices.length>=4) {
			s = s.parallel();
		}
		List<Object> result = s.mapToObj(i->{
			// url index and type parameters and in-query parameters
			ESViewQuery es1Query = getInstanceQuery(query, fEsIndices[i]);
			Object es1Response = null;

			// process requests and run through filters
			// forward request to Elasticsearch instances
			// don't send empty queries
			if (!es1Query.isCancelled()) {
				
				try {
					es1Response = sendEsRequest(es1Query, i);
					query.setResponseFormat(es1Query.getResponseFormat());
					query.setResponseRootPath(es1Query.getResponseRootPath());
				} catch (Exception e) {
					es1Response = makeObjectNode(e, 404);
				}
			}
			
        	return es1Response;
        	
        }).filter(r->!Objects.isNull(r)).collect(Collectors.toList()); 
		
		
		
		// merge results
		// limit returned amount if size is specified		
		
		String response = mergeQueryResultResponses(result, query);
		
		return response;	
	}

	protected ESQuery getInstanceQuery(ESQuery query, Set<String> availIndices) {
		ESQuery esQuery = new ESQuery(query);

		ObjectNode request = query.getQuery();
		List<String> indices = query.getIndexNames();

		// only leave indices which are on this node
		boolean removed = false;
		String indicesFrag = "";
		for (String index : indices) {
			if (availIndices.contains(ESConstants.ALL_FRAGMENT) || availIndices.contains(ESConstants.WILDCARD) || availIndices.contains(index) || index.equals(ESConstants.ALL_FRAGMENT)) {
				indicesFrag += index + ",";
			} else {
				removed = true;
			}
		}
		if (indicesFrag.length() > 0) {
			indicesFrag = indicesFrag.substring(0, indicesFrag.length() - 1);
			esQuery.setIndices(indicesFrag);
		} else if (removed) {
			// all indices were removed - cancel
			esQuery.cancel();
		}

		return esQuery;
	}
	
	protected ESDelete getInstanceQuery(ESDelete query, Set<String> availIndices) {
		ESDelete esQuery = new ESDelete(query);
		
		List<String> indices = query.getIndexNames();

		// only leave indices which are on this node
		boolean removed = false;
		String indicesFrag = "";
		for (String index : indices) {
			if (availIndices.contains(ESConstants.ALL_FRAGMENT) || availIndices.contains(ESConstants.WILDCARD) || availIndices.contains(index) || index.equals(ESConstants.ALL_FRAGMENT)) {
				indicesFrag += index + ",";
			} else {
				removed = true;
			}
		}
		if (indicesFrag.length() > 0) {
			indicesFrag = indicesFrag.substring(0, indicesFrag.length() - 1);
			esQuery.setIndices(indicesFrag);
		} else if (removed) {
			// all indices were removed - cancel
			esQuery.cancel();
		}

		return esQuery;
	}
	

	protected ESViewQuery getInstanceQuery(ESViewQuery query, Set<String> availIndices) {
		ESViewQuery esQuery = new ESViewQuery(query);
		String request = query.getSQL();		
		List<String> indices = List.of(query.getSchema()==null?query.getName():query.getSchema());

		// only leave indices which are on this node
		boolean removed = false;
		String indicesFrag = "";
		for (String index : indices) {
			if (availIndices.contains(ESConstants.ALL_FRAGMENT) || availIndices.contains("*") || availIndices.contains(index) || index.equals(ESConstants.ALL_FRAGMENT)) {
				indicesFrag += index + ",";
			} else {
				removed = true;
			}
		}
		if (indicesFrag.length() > 0) {
			indicesFrag = indicesFrag.substring(0, indicesFrag.length() - 1);
		} else if (removed) {
			// all indices were removed - cancel
			esQuery.cancel();
		}		

		return esQuery;
	}
	
	/**
	 * 只能更新一个index和type
	 * @param query
	 * @param availIndices
	 * @return
	 * @throws Exception
	 */
	protected ESUpdate getInstanceUpdate(ESUpdate query, Set<String> availIndices) {
		ESUpdate esQuery = new ESUpdate(query);

		ObjectNode request = query.getQuery();
		
		List<String> indices = List.of(esQuery.getIndices());

		// only leave indices which are on this node
		boolean removed = false;
		String indicesFrag = "";
		for (String index : indices) {
			if (availIndices.contains(index) || availIndices.contains("*") || availIndices.contains(ESConstants.ALL_FRAGMENT) ) {
				indicesFrag = index;
				break;
			} else {
				removed = true;
			}
		}
		if (indicesFrag.length() > 0) {
			
		} else if (removed) {
			// all indices were removed - cancel
			esQuery.cancel();
		}

		return esQuery;
	}

	protected Object sendEsRequest(ESUpdate query, int index) throws Exception {
		String esReqUrl = this.fEsUrl[index] + query.getQueryUrl();

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = null;

		if (query.getQuery() != null && !query.getFormat().equals("json")) {
			String requestString = makeUrlParams(query.getQuery());
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending data to " + esReqUrl + ": " + requestString);
			}
			
			es1Response = HttpUtil.sendForm(new URL(esReqUrl+requestString), "POST", "");
		}
		else if (query.getQuery() != null) {
			String requestString = query.getQuery().toString();

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending JSON to " + esReqUrl + ": " + requestString);
			}

			es1Response = HttpUtil.sendJson(new URL(esReqUrl), "POST", requestString);
		} else {
			
			es1Response = HttpUtil.getText(new URL(esReqUrl));

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending GET to " + esReqUrl);
			}
		}

		return es1Response;
	}
	
	protected Object sendEsRequest(ESDelete query, int index) throws Exception {
		String esReqUrl = this.fEsUrl[index] + query.buildQueryURL();

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = "";
		if (fLogRequests) {
			fLogger.log(Level.INFO, "sending DELETE to " + esReqUrl);
		}
		if (!query.getFormat().equals("json")) {			
			
			es1Response = HttpUtil.sendForm(new URL(esReqUrl), "DELETE", null);
		}
		else { // json format			

			es1Response = HttpUtil.sendJson(new URL(esReqUrl), "DELETE", null);
		} 

		return es1Response;
	}
	
	protected Object sendEsRequest(ESQuery query, int index) throws Exception {
		
		String esReqUrl = this.fEsUrl[index] + query.buildQueryURL();

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = null;

		if (query.getQuery() != null && !query.getFormat().equals("json")) {
			String requestString = makeUrlParams(query.getQuery());
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending data to " + esReqUrl + ": " + requestString);
			}
			
			es1Response = HttpUtil.sendForm(new URL(esReqUrl), "GET", requestString);
		}
		else if (query.getQuery() != null && query.getFormat().equals("json")) { // json format
			String requestString = query.getQuery().toString();

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending JSON to " + esReqUrl + ": " + requestString);
			}

			es1Response = HttpUtil.sendJson(new URL(esReqUrl), "POST", requestString);
		} else {
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending GET to " + esReqUrl);
			}
			
			es1Response = HttpUtil.getText(new URL(esReqUrl));
		}

		return es1Response;
	}
	
	protected Object sendEsRequest(ESViewQuery query, int index) throws Exception {
		String esReqUrl = this.fEsUrl[index];

		String es1Response = null;
		String requestString = query.buildQueryURL();
		esReqUrl+=requestString;
		
		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");
		
		if (!query.getFormat().equals("json")) {			
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending cmd to " + esReqUrl);
			}
			
			es1Response = HttpUtil.sendForm(new URL(esReqUrl), "GET", null);
		}
		else if (query.getSQL() != null && query.getFormat().equals("json")) {			
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending sql to " + esReqUrl);
			}			
			es1Response = HttpUtil.sendJson(new URL(esReqUrl), "POST", query.getSQL());
		} else {			

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending GET to " + esReqUrl);
			}
			es1Response = HttpUtil.getText(new URL(esReqUrl));
		}

		return es1Response;
	}
	

	protected String mergeOperationResultResponses(List<Object> es1Response,boolean returnSucess,ResponseFormat format) throws Exception {
		for(int i=0;i<es1Response.size();i++){
			ObjectNode es1Json = makeObjectNode(es1Response.get(i));
			// 哪个节点出现错误，返回哪个
			if (!returnSucess && es1Json.has(ESConstants.R_ERROR)) {
				return es1Json.toPrettyString();
			} 
			// 哪个节点成功，返回哪个
			if (returnSucess && !es1Json.has(ESConstants.R_ERROR)) {
				return es1Json.toPrettyString();
			}
		}		
		return "{}";
	}
	
	protected String mergeQueryResultResponses(List<Object>  es1Response, ESQuery query) throws Exception {
		List<ObjectNode> hits = new LinkedList<ObjectNode>();
		int totalHits = 0;
		int shards = 0;
		int limit = query.getLimit();
		int offset = query.getFrom();
		String rootPath = query.getResponseRootPath();
		ResponseFormat format = query.getResponseFormat();
		
		for(int i=0;i<es1Response.size();i++){
			// decode from string
			ObjectNode es1Json = makeObjectNode(es1Response.get(i));			
			ESResponse es1Resp = new ESResponse();
			if (!es1Json.has(ESConstants.R_ERROR)) {
				es1Resp = new ESResponse(es1Json,rootPath);
				shards += es1Resp.getShards();
			} else {
				continue;
			}
			// mix results 50:50 as far as possible
			Iterator<ObjectNode> es1Hits = es1Resp.getHits().iterator();			
	
			while (es1Hits.hasNext()) {
				addHit(hits, es1Hits.next());
			}
			totalHits+=es1Resp.getTotalHits();
		}
		
		if(format==ResponseFormat.OPERATION) {
			if(rootPath==null) {
				return ESRelay.objectMapper.writer().writeValueAsString(hits.get(0));
			}
			else {
				ObjectNode res = ESRelay.jsonNodeFactory.objectNode();
				
				if(hits.size()>=1) {				
					res.set(rootPath, hits.get(0));
				}
				else {
					res.set(rootPath, ESRelay.jsonNodeFactory.objectNode());
				}
				return ESRelay.objectMapper.writer().writeValueAsString(res);
			}
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
		mergedResponse.setSkip(this.fEsIndices.length-es1Response.size());
		mergedResponse.setFail(es1Response.size()-shards);
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

	

	protected void addHit(List<ObjectNode> hits, ObjectNode hit) throws Exception {
		// retrieve type and handle postprocessing
		String type = hit.has(ESConstants.R_HIT_TYPE)? hit.get(ESConstants.R_HIT_TYPE).asText():"_doc";

		List<IPostProcessor> pp = fPostProcs.get(type);
		if (pp != null) {
			for(IPostProcessor gpp : pp) {
				if(hit!=null) {
					hit = gpp.process(hit);
				}
			}
		}

		// postprocessors active for all types
		for (IPostProcessor gpp : fGlobalPostProcs) {
			if(hit!=null) {
				hit = gpp.process(hit);
			}
		}
		
		if(hit!=null) {
			hits.add(hit);
		}
	}

	protected ESQuery removeNestedFilters(ESQuery query) throws Exception {
		ArrayNode andArray = ESUtil.getOrCreateFilterArray(query);

		// json library has no remove function - reconstruct
		ArrayNode newArray = new ArrayNode(ESRelay.jsonNodeFactory);

		// filter out statements that ES 2 can't handle
		for (int i = 0; i < andArray.size(); ++i) {
			boolean keep = true;
			ObjectNode filter = (ObjectNode)andArray.get(i);

			// case 1: nested filter added directly
			if (filter.has(ESConstants.Q_NESTED_FILTER)) {
				keep = false;
			}
			// case 2: nested filter in Nuxeo type or array
			else if (keep && filter.has(ESConstants.Q_OR)) {
				ArrayNode orArr = filter.withArray(ESConstants.Q_OR);
				for (int j = 0; j < orArr.size(); ++j) {
					JsonNode orOjb = orArr.get(j);

					if (orOjb.has(ESConstants.Q_NESTED_FILTER)) {
						keep = false;
						break;
					}
				}
			}

			// not filtered out, add to new array
			if (keep) {
				newArray.add(filter);
			}
		}

		// replace existing with filtered array
		ESUtil.replaceFilterArray(query, newArray);

		return query;
	}

	protected ESQuery handleFiltering(String user, ESQuery query, IFilter blacklist) {
		ObjectNode request = query.getQuery();		
		String action = query.getAction();
		List<String> indices = query.getIndexNames();
		List<String> types = query.getTypeNames();
		if(indices.isEmpty()) {
			return query;
		}
		// remove or block blacklisted indices and types
		UserPermSet perms = fPermCrawler.getPermissions(user);

		if (perms == null) {
			fLogger.log(Level.WARNING, "user '" + user + "' not found in permissions cache");
			perms = new UserPermSet(user);
		}		

		if(action.equals(ESConstants.SEARCH_FRAGMENT) || action.equals(ESConstants.ALL_FRAGMENT)){
			query = blacklist.addFilter(perms, query, indices, types);

			// abort if query is already cancelled through blacklisting
			if (query.isCancelled()) {
				return query;
			}

			// security and visibility filtering
			boolean allIndices = false;
			boolean allTypes = false;

			// check if all indices are to be searched
			if (indices.isEmpty() || indices.size() == 1
					&& (indices.get(0).equals(ESConstants.ALL_FRAGMENT) || indices.get(0).equals(ESConstants.WILDCARD))) {
				allIndices = true;
			}

			// check if all types are to be searched
			if (types.isEmpty() || types.size() == 1 && types.get(0).equals(ESConstants.ALL_FRAGMENT)) {
				allTypes = true;
			}

			// modify query accordingly
			IFilter filter = null;
			if (!allTypes) {
				// search over specific types
				// TODO: types should be sufficient as indicator
				for (String type : types) {
					filter = fTypeFilters.get(type);
					if (filter != null) {
						query = filter.addFilter(perms, query, indices, types);
					}
				}
			} else if (!allIndices && !indices.contains(ESConstants.WILDCARD)) {
				// search over specific indices
				for (String index : indices) {
					filter = fIndexFilters.get(index);
					if (filter != null) {
						query = filter.addFilter(perms, query, indices, types);
					}
				}
			} else {
				// search over all indices and types
				// TODO: exclude filters for the other ES instance
				for (IFilter iFilter : fIndexFilters.values()) {
					query = iFilter.addFilter(perms, query, indices, types);
				}
			}

			// integrate "or" filter array into body if filled
			ArrayNode authFilters = query.getAuthFilterOrArr();
			if (authFilters.size() > 0) {
				ArrayNode filters = ESUtil.getOrCreateFilterArray(query);

				ObjectNode authOr = new ObjectNode(ESRelay.jsonNodeFactory);
				authOr.set(ESConstants.Q_OR, authFilters);

				filters.add(authOr);
			}
		}
		

		return query;
	}	
	

	protected int getLimit(ESViewQuery query) {
		int limit = Integer.MAX_VALUE;

		String[] limitParam = query.getParams().get(ESConstants.MAX_ELEM_PARAM);
		if (limitParam != null) {
			limit = Integer.parseInt(limitParam[0]);
		}

		return limit;
	}

	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {
		fPermCrawler.stop();
	}
	
	/**
	 * Creates URL parameters.
	 *
	 * @param $map URL parameters.
	 * @return void
	 */
	protected String makeUrlParams(ObjectNode query) {
	    StringBuilder $urlParams = new StringBuilder();
	    Iterator<Entry<String, JsonNode>> it = query.fields();
	    while(it.hasNext()){
	    	Entry<String, JsonNode> entry = it.next();
	    	String $key =  entry.getKey();
	    	String $value;
			try {
				$value = entry.getValue().toString();
				
				$urlParams.append('&').append($key).append("=").append(URLEncoder.encode($value,"utf-8"));
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    return $urlParams.toString();
	}
	
	protected ObjectNode makeObjectNode(Object es1Response){
		ObjectNode es1Json = null;
		if (es1Response instanceof ObjectNode) {			
			es1Json = (ObjectNode) es1Response;			
		}
		else if(es1Response instanceof CharSequence){
			try {
				es1Json = (ObjectNode)ESRelay.objectMapper.readTree(es1Response.toString());
			} catch (JsonProcessingException e) {
				ObjectNode value = ESRelay.jsonNodeFactory.objectNode();
				value.put("data", es1Response.toString());
				es1Json = value;
			}
		}
		else if(es1Response instanceof Cache.Entry){
			Cache.Entry<Object,BinaryObject> kv = (Cache.Entry)es1Response;			
			BinaryObject source = (BinaryObject)kv.getValue();
			ObjectNode value = ESRelay.jsonNodeFactory.objectNode();
			value.put("_index", source.type().typeName());
			value.put("_type", "_doc");
			value.putPOJO("_id", kv.getKey());
			value.put("_score", 1.0f);
			ObjectNode sourceJson = ESRelay.objectMapper.convertValue(source, ObjectNode.class);
			value.set("_source", sourceJson);
			es1Json = value;
		}
		else {
			es1Json = ESRelay.objectMapper.convertValue(es1Response, ObjectNode.class);
		}
		return es1Json;
	}
	
	protected ObjectNode makeObjectNode(Exception e, int statusCode) {
		ObjectNode error = ESRelay.jsonNodeFactory.objectNode();
		error.put(ESConstants.R_ERROR, e.getMessage()+" cause by "+e.getClass());
		error.put(ESConstants.R_STATUS,statusCode);
		return error;
	}
}
