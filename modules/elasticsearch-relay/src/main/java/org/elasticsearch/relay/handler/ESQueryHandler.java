package org.elasticsearch.relay.handler;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ESRelayConfig;

import org.elasticsearch.relay.filters.BlacklistFilter;
import org.elasticsearch.relay.filters.IFilter;
import org.elasticsearch.relay.filters.ImapFilter;
import org.elasticsearch.relay.filters.LiferayFilter;
import org.elasticsearch.relay.filters.NuxeoFilter;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;



/**
 * Central query handler splitting up queries between multiple ES instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryHandler {
	
	final PermissionCrawler fPermCrawler;
	
	protected final String fEsUrl;
	protected final String fEs2Url;

	protected final Set<String> fEs1Indices;
	protected final Set<String> fEs2Indices;

	

	protected final Map<String, IFilter> fIndexFilters, fTypeFilters;

	//对搜索结果进行处理
	protected final MultiValueMap<String, IPostProcessor> fPostProcs;
	protected final List<IPostProcessor> fGlobalPostProcs;

	protected final BlacklistFilter fEs1BlacklistFilter;
	protected final BlacklistFilter fEs2BlacklistFilter;

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

		fEsUrl = config.getElasticUrl();
		fEs2Url = config.getEs2ElasticUrl();

		fEs1Indices = config.getElasticIndices();
		fEs2Indices = config.getEs2Indices();
		
		fLogger = Logger.getLogger(this.getClass().getName());

		// initialize permission crawlers
		List<IPermCrawler> crawlers = new ArrayList<IPermCrawler>();

		crawlers.add(new NuxeoCrawler(config.getNuxeoUrl(), config.getNuxeoUser(), config.getNuxeoPassword()));

		crawlers.add(new LiferayCrawler(config.getLiferayUrl(), config.getLiferayCompanyId(), config.getLiferayUser(),
				config.getLiferayPassword()));

		String permGetUrl = config.getPermissionsCrawlUrl();
		fPermCrawler = new PermissionCrawler(permGetUrl, crawlers, config.getPermCrawlInterval());
		
		if(config.getPermCrawlInterval()>= 0){			
			Thread pcThread = new Thread(fPermCrawler);
			//pcThread.setDaemon(true);
			pcThread.start();
		}

		fIndexFilters = new HashMap<String, IFilter>();
		fTypeFilters = new HashMap<String, IFilter>();

		// TODO: really initialize filters here?
		fEs1BlacklistFilter = new BlacklistFilter(config.getEs1BlacklistIndices(), config.getEs1BlacklistTypes());
		fEs2BlacklistFilter = new BlacklistFilter(config.getEs2BlacklistIndices(), config.getEs2BlacklistTypes());

		ImapFilter mailFilter = new ImapFilter(config.getMailTypes());
		fIndexFilters.put(config.getMailIndex(), mailFilter);
		for (String type : config.getMailTypes()) {
			fTypeFilters.put(type, mailFilter);
		}

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
		String es1Response = null;
		String es2Response = null;

		// url index and type parameters and in-query parameters

		// split queries between ES instances, cancel empty queries
		ESUpdate es1Query = getInstanceUpdate(query, fEs1Indices);
		ESUpdate es2Query = getInstanceUpdate(query, fEs2Indices);

		// process requests and run through filters
		// forward request to Elasticsearch instances
		// don't send empty queries
		if (!es1Query.isCancelled()) {
			
			es1Response = sendEsRequest(es1Query, fEsUrl);
		}
		if (!es2Query.isCancelled()) {
			// remove incompatible nested path filter		
			es2Response = sendEsRequest(es2Query, fEs2Url);
		}
		
		if(es2Response==null){
			return es1Response;
		}

		if(es1Response==null){
			return es2Response;
		}
		
		//TODO merge results
		String response = es1Response + "\n\n"+ es2Response;

		return response.toString();
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
		String es1Response = null;
		String es2Response = null;

		// url index and type parameters and in-query parameters

		// split queries between ES instances, cancel empty queries
		ESQuery es1Query = getInstanceQuery(query, fEs1Indices);
		ESQuery es2Query = getInstanceQuery(query, fEs2Indices);

		// process requests and run through filters
		// forward request to Elasticsearch instances
		// don't send empty queries
		if (!es1Query.isCancelled()) {
			es1Query = handleFiltering(user, es1Query, fEs1BlacklistFilter);
			es1Response = sendEsRequest(es1Query, fEsUrl);
		}
		if (!es2Query.isCancelled()) {
			// remove incompatible nested path filter
			es2Query = removeNestedFilters(es2Query);

			es2Query = handleFiltering(user, es2Query, fEs2BlacklistFilter);
			es2Response = sendEsRequest(es2Query, fEs2Url);
		}
		
		
		// merge results
		// limit returned amount if size is specified
		int limit = getLimit(query);

		String response = mergeResponses(es1Response, es2Response, limit);

		return response;
	}
	
	public String handleRequest(ESViewQuery query, String user) throws Exception {
		String es1Response = null;
		String es2Response = null;

		// url index and type parameters and in-query parameters

		// split queries between ES instances, cancel empty queries
		ESViewQuery es1Query = getInstanceQuery(query, fEs1Indices);
		ESViewQuery es2Query = getInstanceQuery(query, fEs2Indices);

		// process requests and run through filters
		// forward request to Elasticsearch instances
		// don't send empty queries
		if (!es1Query.isCancelled()) {
			//es1Query = handleFiltering(user, es1Query, fEs1BlacklistFilter);
			es1Response = sendEsRequest(es1Query, fEsUrl);
		}
		if (!es2Query.isCancelled()) {
			// remove incompatible nested path filter
			//es2Query = removeNestedFilters(es2Query);

			//es2Query = handleFiltering(user, es2Query, fEs2BlacklistFilter);
			es2Response = sendEsRequest(es2Query, fEs2Url);
		}		
		
		int limit = Integer.MAX_VALUE;

		String response = mergeResponses(es1Response, es2Response,limit);

		return response;
	}

	protected ESQuery getInstanceQuery(ESQuery query, Set<String> availIndices) throws Exception {
		ESQuery esQuery = new ESQuery();

		ObjectNode request = query.getQuery();
		String[] path = query.getQueryPath();
		List<String> indices = getIndexNames(path);

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

		// TODO: only works if there is an actual path
		String[] newPath = path.clone();
		newPath[0] = indicesFrag;
		esQuery.setQueryPath(newPath);

		// TODO: also filter parameters and request body
		esQuery.setParams(query.getParams());

		// TODO: safely duplicate query body
		if (query.getQuery() != null) {
			//ObjectNode newQueryObj = new ObjectNode(query.getQuery().toString());
			esQuery.setQuery(query.getQuery());
		}

		return esQuery;
	}
	

	protected ESViewQuery getInstanceQuery(ESViewQuery query, Set<String> availIndices) throws Exception {
		ESViewQuery esQuery = new ESViewQuery();

		String request = query.getSQL();
		String[] path = query.getQueryPath();
		List<String> indices = getIndexNames(path);

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

		// TODO: only works if there is an actual path
		String[] newPath = path.clone();
		newPath[0] = indicesFrag;
		esQuery.setQueryPath(newPath);

		// TODO: also filter parameters and request body
		esQuery.setParams(query.getParams());

		// TODO: safely duplicate query body
		if (query.getSQL() != null) {		
			esQuery.setSQL(query.getSQL());
		}
		
		if (query.getNamedSQL() != null) {		
			esQuery.setNamedSQL(query.getNamedSQL());
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
	protected ESUpdate getInstanceUpdate(ESUpdate query, Set<String> availIndices) throws Exception {
		ESUpdate esQuery = new ESUpdate();

		ObjectNode request = query.getQuery();
		String[] path = query.getQueryPath();
		List<String> indices = getIndexNames(path);

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

		// TODO: only works if there is an actual path
		String[] newPath = path.clone();
		newPath[0] = indicesFrag;
		esQuery.setQueryPath(newPath);

		// TODO: also filter parameters and request body
		esQuery.setParams(query.getParams());
		esQuery.setOp(query.getOp());
		esQuery.setFormat(query.getFormat());
		// TODO: safely duplicate query body
		if (query.getQuery() != null) {			
			esQuery.setQuery(query.getQuery());
		}

		return esQuery;
	}

	protected String sendEsRequest(ESUpdate query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getQueryUrl();

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
	
	protected String sendEsRequest(ESQuery query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getQueryUrl();

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
		else if (query.getQuery() != null) {
			String requestString = query.getQuery().toString();

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending JSON to " + esReqUrl + ": " + requestString);
			}

			es1Response = HttpUtil.sendJson(new URL(esReqUrl), "GET", requestString);
		} else {
			es1Response = HttpUtil.getText(new URL(esReqUrl));

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending GET to " + esReqUrl);
			}
		}

		return es1Response;
	}
	
	protected String sendEsRequest(ESViewQuery query, String esUrl) throws Exception {
		String esReqUrl = esUrl + query.getSQL();

		// replace spaces since they cause problems with proxies etc.
		esReqUrl = esReqUrl.replaceAll(" ", "%20");

		String es1Response = null;

		if (query.getSQL() != null && !query.getFormat().equals("json")) {
			String requestString = query.getSQL();
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending sql to " + esReqUrl);
			}
			
			es1Response = HttpUtil.sendForm(new URL(esReqUrl), "GET", null);
		}
		else if (query.getSQL() != null) {
			String requestString = query.getSQL();
			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending sql to " + esReqUrl);
			}

			es1Response = HttpUtil.sendJson(new URL(esReqUrl), "GET", null);
		} else {
			es1Response = HttpUtil.getText(new URL(esReqUrl));

			if (fLogRequests) {
				fLogger.log(Level.INFO, "sending GET to " + esReqUrl);
			}
		}

		return es1Response;
	}

	protected String mergeResponses(String es1Response, String es2Response, int limit) throws Exception {
		
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

	protected void addHit(List<ObjectNode> hits, ObjectNode hit) throws Exception {
		// retrieve type and handle postprocessing
		String type = hit.get(ESConstants.R_HIT_TYPE).asText();

		List<IPostProcessor> pp = fPostProcs.get(type);
		if (pp != null) {
			for(IPostProcessor gpp : pp) {
				hit = gpp.process(hit);
			}
		}

		// postprocessors active for all types
		for (IPostProcessor gpp : fGlobalPostProcs) {
			hit = gpp.process(hit);
		}

		hits.add(hit);
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

	protected ESQuery handleFiltering(String user, ESQuery query, IFilter blacklist) throws Exception {
		ObjectNode request = query.getQuery();
		String[] path = query.getQueryPath();

		List<String> indices = getIndexNames(path);
		List<String> types = getTypeNames(path);

		// remove or block blacklisted indices and types
		UserPermSet perms = fPermCrawler.getPermissions(user);

		if (perms == null) {
			fLogger.log(Level.WARNING, "user '" + user + "' not found in permissions cache");
			perms = new UserPermSet(user);
		}

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
			authOr.put(ESConstants.Q_OR, authFilters);

			filters.add(authOr);
		}

		return query;
	}

	protected List<String> getIndexNames(String[] path) {
		List<String> indices = new ArrayList<String>();

		// extract from path
		if (path.length > 0) {
			String names = path[0];
			if (names != null && !names.isEmpty()) {
				if (names.contains(",")) {
					String[] nameArr = names.split(",");
					for (String n : nameArr) {
						indices.add(n);
					}
				} else {
					indices.add(names);
				}
			}
		}

		// TODO: extract from body

		return indices;
	}

	protected List<String> getTypeNames(String[] path) {
		List<String> types = new ArrayList<String>();

		// extract from path
		if (path.length > 1) {
			String names = path[1];
			if (names != null && !names.isEmpty()) {
				if (names.contains(",")) {
					String[] nameArr = names.split(",");
					for (String n : nameArr) {
						types.add(n);
					}
				} else {
					types.add(names);
				}
			}
		}

		// TODO: extract from body

		return types;
	}

	protected int getLimit(ESQuery query) {
		int limit = Integer.MAX_VALUE;

		String limitParam = query.getParams().get(ESConstants.MAX_ELEM_PARAM);
		if (limitParam != null) {
			limit = Integer.parseInt(limitParam);
		}

		ObjectNode queryObj = query.getQuery();
		if (queryObj != null && queryObj.has(ESConstants.MAX_ELEM_PARAM)) {
			String requestLimit = queryObj.get(ESConstants.MAX_ELEM_PARAM).asText();
			if (requestLimit != null && !requestLimit.isEmpty()) {
				try {
					int reqLim = Integer.parseInt(requestLimit);

					limit = Math.min(limit, reqLim);
				} catch (Exception e) {
					fLogger.log(Level.WARNING, "invalid size limit: " + requestLimit);
				}
			}
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
}
