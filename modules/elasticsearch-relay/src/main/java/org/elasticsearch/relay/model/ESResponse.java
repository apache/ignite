package org.elasticsearch.relay.model;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.relay.util.ESConstants;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * List of JSONObjects representing an Elasticsearch result, with total number
 * of shards and results stored separately.
 */
public class ESResponse {
	private final List<JSONObject> fHits;

	// TODO: actually note their status and not just assume a success?
	private int fShards = 0;

	private int fTotalResults = 0;

	public ESResponse() {
		this(new ArrayList<JSONObject>());
	}

	public ESResponse(List<JSONObject> hits) {
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
	public ESResponse(JSONObject body) throws Exception {
		fHits = new ArrayList<JSONObject>();

		if (body != null) {
			JSONObject hitsObj = body.optJSONObject(ESConstants.R_HITS);
			if (hitsObj != null && hitsObj.optJSONArray(ESConstants.R_HITS) != null) {
				addHits(hitsObj.optJSONArray(ESConstants.R_HITS));

				fTotalResults = hitsObj.optInt(ESConstants.R_HITS_TOTAL);
			}

			if (body.optJSONObject(ESConstants.R_SHARDS) != null) {
				fShards = body.getJSONObject(ESConstants.R_SHARDS).getInt(ESConstants.R_SHARDS_TOT);
			}
		}
	}

	private void addHits(JSONArray hits) throws Exception {
		final int size = hits.length();

		for (int i = 0; i < size; ++i) {
			fHits.add(hits.getJSONObject(i));
		}
	}

	public List<JSONObject> getHits() {
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
	public JSONObject toJSON() throws Exception {
		JSONObject result = new JSONObject();

		// TODO: took and timed_out?

		// shards
		JSONObject shardsObj = new JSONObject();

		shardsObj.put(ESConstants.R_SHARDS_TOT, fShards);
		shardsObj.put(ESConstants.R_SHARDS_SUC, fShards);
		shardsObj.put(ESConstants.R_SHARDS_FAIL, 0);

		result.put(ESConstants.R_SHARDS, shardsObj);

		// hits
		JSONObject hitsObj = new JSONObject();
		hitsObj.put(ESConstants.R_HITS_TOTAL, fTotalResults);

		// actual hit entries
		JSONArray hits = new JSONArray();
		for (JSONObject hit : fHits) {
			hits.put(hit);
		}
		hitsObj.put(ESConstants.R_HITS, hits);

		result.put(ESConstants.R_HITS, hitsObj);

		return result;
	}
}
