package org.elasticsearch.relay.model;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.util.ESConstants;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * List of ObjectNodes representing an Elasticsearch result, with total number
 * of shards and results stored separately.
 */
public class ESResponse {
	
	private final List<ObjectNode> fHits;

	// TODO: actually note their status and not just assume a success?
	private int fShards = 1;
	
	private int fSkip = 0;	
	
	private int fFail = 0;

	private int fTotalResults = 0;
	
	private long took = 0;
	
	private boolean timed_out = false;

	public ESResponse() {
		this(new ArrayList<ObjectNode>());
	}

	public ESResponse(List<ObjectNode> hits) {
		fHits = hits;

		fTotalResults = hits.size();
		
		took = System.currentTimeMillis();
	}

	/**
	 * Constructs a result object from the given body received from
	 * Elasticsearch. Collects all result objects and stores the number of total
	 * hits and shards.
	 * 
	 * @param body
	 *            result object sent by Elasticsearch
	 * @throws Exception
	 *             if disassembly fails
	 */
	public ESResponse(ObjectNode body,String rootPath) throws Exception {
		fHits = new ArrayList<ObjectNode>();
		took = System.currentTimeMillis();
		if (body != null && body.has(ESConstants.R_HITS)) {
			ObjectNode hitsObj = body.withObject("/"+ESConstants.R_HITS);
			if (hitsObj != null && hitsObj.withArray("/"+ESConstants.R_HITS) != null) {
				addHits(hitsObj.withArray("/"+ESConstants.R_HITS));

				fTotalResults = hitsObj.get(ESConstants.R_HITS_TOTAL).get("value").asInt(0);
			}

			if (body.withObject("/"+ESConstants.R_SHARDS) != null) {
				fShards = body.withObject("/"+ESConstants.R_SHARDS).get("/"+ESConstants.R_SHARDS_TOT).asInt(0);
			}
		}
		else if (body != null && body.has("response")) {
			ObjectNode hitsObj = body.withObject("/response");
			if (hitsObj != null && hitsObj.has("items")) {
				addHits(hitsObj.withArray("/items"));

				fTotalResults = this.getHits().size();
			}

			if (hitsObj!=null && hitsObj.has("fieldsMetadata")) {
				ArrayNode fields = hitsObj.withArray("/fieldsMetadata");
			}
			
			if (hitsObj != null && hitsObj.has(rootPath)) {
				addHits(hitsObj.withArray("/"+rootPath));

				fTotalResults = this.getHits().size();
			}
		}
		else if (rootPath!=null && body != null && body.has(rootPath)) {			
			addHits(body.withArray("/"+rootPath));
			fTotalResults = this.getHits().size();
		}
		else {
			fHits.add(body);
		}
	}

	private void addHits(ArrayNode hits) throws Exception {
		final int size = hits.size();

		for (int i = 0; i < size; ++i) {
			fHits.add((ObjectNode)hits.get(i));
		}
	}

	public List<ObjectNode> getHits() {
		return fHits;
	}

	public int getShards() {
		return fShards;
	}

	public void setShards(int shards) {
		fShards = shards;
	}

	public int getTotalHits() {
		if(fTotalResults==0 && fHits!=null) {
			return fHits.size();
		}
		return fTotalResults;
	}

	public void setTotalHits(int hits) {
		fTotalResults = hits;
	}
	
	public int getSkip() {
		return fSkip;
	}

	public void setSkip(int fSkip) {
		this.fSkip = fSkip;
	}

	public int getFail() {
		return fFail;
	}

	public void setFail(int fFail) {
		this.fFail = fFail;
	}

	public long getTook() {
		return took;
	}

	public void setTook(long took) {
		this.took = took;
	}

	public boolean isTimed_out() {
		return timed_out;
	}

	public void setTimed_out(boolean timed_out) {
		this.timed_out = timed_out;
	}


	/**
	 * @return reassembles an Elasticsearch result body
	 * @throws Exception
	 *             if assembly fails
	 */
	public ObjectNode toJSON() throws Exception {
		ObjectNode result = new ObjectNode(ESRelay.jsonNodeFactory);
		if(this.took>100000000) {
			long endTime = System.currentTimeMillis();
			this.took = endTime - this.took;
		}
		
		// took and timed_out?
		result.put("took", this.took);
		
		result.put("timed_out", this.timed_out);

		// shards
		ObjectNode shardsObj = new ObjectNode(ESRelay.jsonNodeFactory);

		shardsObj.put(ESConstants.R_SHARDS_TOT, fShards+fFail);
		shardsObj.put(ESConstants.R_SHARDS_SUC, fShards);
		shardsObj.put(ESConstants.R_SHARDS_FAIL, fFail);
		shardsObj.put(ESConstants.R_SHARDS_SKIP, fSkip);

		result.set(ESConstants.R_SHARDS, shardsObj);
		//-result.set(ESConstants.R_CLUSTERS, shardsObj);

		// hits
		ObjectNode hitsObj = new ObjectNode(ESRelay.jsonNodeFactory);
		ObjectNode totalObj = new ObjectNode(ESRelay.jsonNodeFactory);
		
		totalObj.put("value", fTotalResults);
		totalObj.put("relation", "eq");		
		hitsObj.set(ESConstants.R_HITS_TOTAL, totalObj);		
		
		double maxScore = 0;

		// actual hit entries
		ArrayNode hits = new ArrayNode(ESRelay.jsonNodeFactory,fHits.size());
		for (ObjectNode hit : fHits) {
			hits.add(hit);
			if(hit.has("_score")) {
				double score = hit.get("_score").asDouble(0.0);
				if(score>maxScore) {
					maxScore = score;
				}
			}
		}
		
		hitsObj.put("max_score", maxScore);
		hitsObj.set(ESConstants.R_HITS, hits);

		result.set(ESConstants.R_HITS, hitsObj);

		return result;
	}
	
	/**
	 * @return reassembles an Dataset result body
	 * @throws Exception
	 *             if assembly fails
	 */
	public ObjectNode toDataset(String path) throws Exception {
		ObjectNode result = new ObjectNode(ESRelay.jsonNodeFactory);

		// hits		
		result.put("shards",fShards);
		result.put("total",fTotalResults);

		// actual hit entries
		ArrayNode hits = new ArrayNode(ESRelay.jsonNodeFactory,fHits.size());
		for (ObjectNode hit : fHits) {
			hits.add(hit);
		}
		if(path==null || path.isEmpty()) {
			path = "items";
		}
		result.set(path, hits);

		return result;
	}
}
