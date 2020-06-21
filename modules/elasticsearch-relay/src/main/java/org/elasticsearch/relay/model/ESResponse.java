package org.elasticsearch.relay.model;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.util.ESConstants;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * List of ObjectNodes representing an Elasticsearch result, with total number
 * of shards and results stored separately.
 */
public class ESResponse {
	private final List<ObjectNode> fHits;

	// TODO: actually note their status and not just assume a success?
	private int fShards = 0;

	private int fTotalResults = 0;

	public ESResponse() {
		this(new ArrayList<ObjectNode>());
	}

	public ESResponse(List<ObjectNode> hits) {
		fHits = hits;

		fTotalResults = hits.size();
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
	public ESResponse(ObjectNode body) throws Exception {
		fHits = new ArrayList<ObjectNode>();

		if (body != null && body.has(ESConstants.R_HITS)) {
			ObjectNode hitsObj = body.with(ESConstants.R_HITS);
			if (hitsObj != null && hitsObj.withArray(ESConstants.R_HITS) != null) {
				addHits(hitsObj.withArray(ESConstants.R_HITS));

				fTotalResults = hitsObj.get(ESConstants.R_HITS_TOTAL).asInt(0);
			}

			if (body.with(ESConstants.R_SHARDS) != null) {
				fShards = body.with(ESConstants.R_SHARDS).get(ESConstants.R_SHARDS_TOT).asInt(0);
			}
		}
		else if (body != null && body.has("response")) {
			ObjectNode hitsObj = body.with("response");
			if (hitsObj != null && hitsObj.has("items")) {
				addHits(hitsObj.withArray("items"));

				fTotalResults = this.getHits().size();
			}

			if (hitsObj!=null && hitsObj.has("fieldsMetadata")) {
				fShards = hitsObj.get("queryId").asInt(0);
			}
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
		return fTotalResults;
	}

	public void setTotalHits(int hits) {
		fTotalResults = hits;
	}

	/**
	 * @return reassembles an Elasticsearch result body
	 * @throws Exception
	 *             if assembly fails
	 */
	public ObjectNode toJSON() throws Exception {
		ObjectNode result = new ObjectNode(ESRelay.jsonNodeFactory);

		// TODO: took and timed_out?

		// shards
		ObjectNode shardsObj = new ObjectNode(ESRelay.jsonNodeFactory);

		shardsObj.put(ESConstants.R_SHARDS_TOT, fShards);
		shardsObj.put(ESConstants.R_SHARDS_SUC, fShards);
		shardsObj.put(ESConstants.R_SHARDS_FAIL, 0);

		result.put(ESConstants.R_SHARDS, shardsObj);

		// hits
		ObjectNode hitsObj = new ObjectNode(ESRelay.jsonNodeFactory);
		hitsObj.put(ESConstants.R_HITS_TOTAL, fTotalResults);

		// actual hit entries
		ArrayNode hits = new ArrayNode(ESRelay.jsonNodeFactory,fHits.size());
		for (ObjectNode hit : fHits) {
			hits.add(hit);
		}
		hitsObj.put(ESConstants.R_HITS, hits);

		result.put(ESConstants.R_HITS, hitsObj);

		return result;
	}
}
