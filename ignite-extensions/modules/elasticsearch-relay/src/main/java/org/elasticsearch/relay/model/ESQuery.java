package org.elasticsearch.relay.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ResponseFormat;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;



/**
 * Elasticsearch query, disassembled into path, parameters and body. Also
 * contains a persistent filtering "or array" to attach filters to that is
 * included in the resulting query. this array is not added to the body
 * automatically (not here at least).
 */
public class ESQuery {
	
	private String format = "json"; // json , form
	
	private ResponseFormat responseFormat = ResponseFormat.HITS;
	
	private String[] fPath;

	private Map<String, String[]> fParams;

	private ObjectNode fBody;

	private ArrayNode fAuthFilterOrArr;

	private boolean fCancelled = false;

	/**
	 * Creates an empty query.
	 */
	private ESQuery() {
		this(null);
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
		this(path, new HashMap<>(), body);
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
		fPath = path;
		
		fBody = body;
		
		fAuthFilterOrArr = new ArrayNode(ESRelay.jsonNodeFactory);
		
		this.setParams(params);
	}

	public String[] getQueryPath() {
		return fPath;
	}

	public void setQueryPath(String[] path) {
		fPath = path;
	}

	public Map<String, String[]> getParams() {
		return fParams;
	}

	public void setParams(Map<String, String[]> params) {
		String[] responseFormat = params.get("responseFormat");
		if(responseFormat!=null) {
			this.setResponseFormat(ResponseFormat.valueOf(responseFormat[0].toUpperCase()));
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
		if (fPath != null) {
			for (String frag : fPath) {
				// skip empty elements
				if (!frag.isEmpty()) {
					urlBuff.append(frag);
					urlBuff.append("/");
				}
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
}
