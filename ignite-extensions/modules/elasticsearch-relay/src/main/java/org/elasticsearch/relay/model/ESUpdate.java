package org.elasticsearch.relay.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.ResponseFormat;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Elasticsearch index, disassembled into path, parameters and body. Also
 * contains a persistent filtering "or array" to attach filters to that is
 * included in the resulting query. this array is not added to the body
 * automatically (not here at least).
 */
public class ESUpdate {
	
	private String format = "json";
	
	private ResponseFormat responseFormat = ResponseFormat.OPERATION;
	
	private String op;
	//PUT /{index}/_doc/{id}
	private String indices; 
	
	private String action;
	
	private String docId; 

	private Map<String, String> fParams;

	private ObjectNode fBody;

	private ArrayNode fAuthFilterOrArr;

	private boolean fCancelled = false;
	

	/**
	 * @param path
	 *            query path
	 */
	public ESUpdate(String[] path) {
		this(path, (ObjectNode) null);
	}

	/**
	 * @param path
	 *            query path
	 * @param body
	 *            query body
	 */
	public ESUpdate(String[] path, ObjectNode body) {
		this(path, null, body);
	}

	/**
	 * @param path
	 *            query path
	 * @param params
	 *            query parameters
	 */
	public ESUpdate(String[] path, Map<String, String> params) {
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
	public ESUpdate(String[] path, Map<String, String> params, ObjectNode body) {
		indices = path[0];
		
		if(path.length>=2) {
			action = path[1];
		}
		
		if(path.length>=3) {
			docId = path[2];
		}
		fParams = params;
		fBody = body;

		fAuthFilterOrArr = new ArrayNode(ESRelay.jsonNodeFactory);
	}
	
	public ESUpdate(ESUpdate copy) {
		indices = copy.indices;
		action = copy.action;
		docId = copy.docId;
		op = copy.op;
		this.format = copy.format;
		this.responseFormat = copy.responseFormat;
		
		fBody = copy.fBody;
		
		fAuthFilterOrArr = new ArrayNode(ESRelay.jsonNodeFactory);
		if(copy.fParams!=null) {
			this.setParams(copy.fParams);
		}
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

	public Map<String, String> getParams() {
		return fParams;
	}

	public void setParams(Map<String, String> params) {
		String responseFormat = params.get("responseFormat");
		if(responseFormat!=null) {
			this.setResponseFormat(ResponseFormat.of(responseFormat));
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

	/**
	 * @return reassembled query URL (without the server)
	 */
	public String getQueryUrl() {
		StringBuffer urlBuff = new StringBuffer();
		
		// reconstruct request path
		if (indices != null) {
			// skip empty elements
			if (!indices.isEmpty()) {
				urlBuff.append("/");
				urlBuff.append(indices);				
			}
		}
		
		if (action != null && !action.equals("_cmd")) {
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
			Iterator<Entry<String, String>> paramIter = fParams.entrySet().iterator();
			Entry<String, String> entry = paramIter.next();

			urlBuff.append("?");
			urlBuff.append(entry.getKey());
			urlBuff.append("=");
			urlBuff.append(entry.getValue());

			while (paramIter.hasNext()) {
				entry = paramIter.next();
				urlBuff.append("&");
				urlBuff.append(entry.getKey());
				urlBuff.append("=");
				urlBuff.append(entry.getValue());
			}
		}

		return urlBuff.toString();
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

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
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
	
	/**
	 * schema.Table
	 * @return Table
	 */
	public String getTypeName() {
		// extract type from path
		String names = this.indices;
		if (names != null && !names.isEmpty()) {
			int pos = names.lastIndexOf('.');
			if(pos>0) {
				return names.substring(pos+1);
			}		
		}

		return names;
	}
}
