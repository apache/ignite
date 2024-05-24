package org.elasticsearch.relay.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ResponseFormat;
import org.elasticsearch.relay.util.ESConstants;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;



/**
 * Elasticsearch query, disassembled into path, parameters and body. Also
 * contains a persistent filtering "or array" to attach filters to that is
 * included in the resulting query. this array is not added to the body
 * automatically (not here at least).
 */
public class ESQuery {
	// input format
	private String format = "json"; // json , form
	
	private ResponseFormat responseFormat = ResponseFormat.HITS;
	
	private String responseRootPath = "items";
	
	//GET {index}/{_action}/{docId}
	
	private String indices; 
	
	private String action;
	
	private String docId;	

	private Map<String, String[]> fParams;

	private ObjectNode fBody;

	private ArrayNode fAuthFilterOrArr;

	private boolean fCancelled = false;
	
	public ESQuery() {
		indices = "";
		action = "_all";
	}
	
	/**
	 * @param path
	 *            query path
	 */
	public ESQuery(String[] path) {
		this(path, (ObjectNode) null);
	}

	/**
	 * @param path
	 *            query path
	 * @param body
	 *            query body
	 */
	public ESQuery(String[] path, ObjectNode body) {
		this(path, new HashMap<String, String[]>(), body);
	}

	/**
	 * @param path
	 *            query path
	 * @param params
	 *            query parameters
	 */
	public ESQuery(String[] path, Map<String, String[]> params) {
		this(path, params, null);
	}

	/**
	 * @param path
	 *            query path
	 * @param params
	 *            query parameters
	 * @param body
	 *            query body
	 */
	public ESQuery(String[] path, Map<String, String[]> params, ObjectNode body) {
		indices = path[0];
		
		if(path.length>=2) {
			action = path[1];
		}
		
		if(path.length>=3) {
			docId = path[2];
		}
		
		fBody = body;
		
		fAuthFilterOrArr = new ArrayNode(ESRelay.jsonNodeFactory);
		
		this.setParams(params);
	}
	
	public ESQuery(ESQuery copy) {
		indices = copy.indices;
		action = copy.action;
		docId = copy.docId;
		
		fBody = copy.fBody;
		
		fAuthFilterOrArr = new ArrayNode(ESRelay.jsonNodeFactory);
		
		this.setParams(copy.fParams);
	}
	

	public Map<String, String[]> getParams() {
		return fParams;
	}
	
	public String param(String... name) {		
		for(String nameOne: name) {
			String[] value = fParams.get(nameOne);
			if(value!=null) {
				return value[0];
			}
		}
		return null;
	}

	public void setParams(Map<String, String[]> params) {
		String[] responseFormat = params.get("responseFormat");
		if(responseFormat!=null) {
			this.setResponseFormat(ResponseFormat.of(responseFormat[0]));
		}
		String[] responseRootPath = params.get("responseRootPath");
		if(responseRootPath!=null && !responseRootPath[0].isBlank()) {
			this.responseRootPath = responseRootPath[0].strip();
		}
		fParams = params;
	}

	public ObjectNode getQuery() {
		return fBody;
	}

	public void setQuery(ObjectNode query) {
		fBody = query;
	}

	public ArrayNode getAuthFilterOrArr() {
		return fAuthFilterOrArr;
	}

	/**
	 * @return whether this query has been cancelled internally
	 */
	public boolean isCancelled() {
		return fCancelled;
	}

	/**
	 * Cancel this query internally - do not process further and do not send.
	 */
	public void cancel() {
		fCancelled = true;
	}


	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public ResponseFormat getResponseFormat() {
		return responseFormat;
	}

	public void setResponseFormat(ResponseFormat responseFormat) {
		this.responseFormat = responseFormat;
	}
	
	

	/**
	 * @return reassembled query URL (without the server)
	 */
	public String buildQueryURL() {
		StringBuffer urlBuff = new StringBuffer();

		// reconstruct request path
		if (indices != null) {
			// skip empty elements
			if (!indices.isEmpty()) {				
				urlBuff.append("/");
				urlBuff.append(indices);
			}
		}
		
		if (action != null) {
			// skip empty elements
			if (!action.isEmpty()) {
				urlBuff.append("/");
				urlBuff.append(action);				
			}
		}
		
		if (docId != null) {
			// skip empty elements
			if (!docId.isEmpty()) {
				urlBuff.append("/");
				urlBuff.append(docId);				
			}
		}

		// add parameters
		if (fParams != null && !fParams.isEmpty()) {
			// construct URL with all parameters
			Iterator<Entry<String, String[]>> paramIter = fParams.entrySet().iterator();
			Entry<String, String[]> entry = null;
			urlBuff.append("?");
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

	public String getIndices() {
		return indices;
	}

	public void setIndices(String indices) {
		this.indices = indices;
	}

	public String getAction() {
		return action;
	}

	public String getDocId() {
		return docId;
	}
	
	public int getLimit() {
		int limit = 1024;

		String[] limitParam = getParams().get(ESConstants.MAX_ELEM_PARAM);
		if (limitParam != null) {
			limit = Integer.parseInt(limitParam[0]);
		}
		else {
			ObjectNode queryObj = getQuery();
			if (queryObj != null && queryObj.has(ESConstants.MAX_ELEM_PARAM)) {
				limit = queryObj.get(ESConstants.MAX_ELEM_PARAM).asInt(limit);				
			}
		}

		return limit;
	}
	
	public int getFrom() {
		int from = 0;

		String[] limitParam = getParams().get(ESConstants.FIRST_ELEM_PARAM);
		if (limitParam != null) {
			from = Integer.parseInt(limitParam[0]);
		}
		else {
			ObjectNode queryObj = getQuery();
			if (queryObj != null  && queryObj.has(ESConstants.FIRST_ELEM_PARAM)) {
				from = queryObj.get(ESConstants.FIRST_ELEM_PARAM).asInt(from);				
			}
		}
		return from;
	}
	
	public List<String> getIndexNames() {
		List<String> indices = new ArrayList<String>();

		// extract from path
		String names = this.indices;
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
		return indices;
	}

	public List<String> getTypeNames() {
		List<String> types = new ArrayList<String>();

		// extract from path
		String names = this.indices;
		if (names != null && !names.isEmpty()) {
			if (names.contains(",")) {
				String[] nameArr = names.split(",");
				for (String n : nameArr) {
					int pos = n.lastIndexOf('.');
					if(pos>0) {
						types.add(n.substring(pos+1));
					}
				}
			} else {
				int pos = names.lastIndexOf('.');
				if(pos>0) {
					types.add(names.substring(pos+1));
				}				
			}
		}

		return types;
	}

	public String getResponseRootPath() {
		return responseRootPath;
	}

	public void setResponseRootPath(String responseRootPath) {
		this.responseRootPath = responseRootPath;
	}
}
