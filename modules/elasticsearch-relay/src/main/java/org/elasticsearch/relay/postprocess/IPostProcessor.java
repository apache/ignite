package org.elasticsearch.relay.postprocess;

import org.json.JSONObject;

/**
 * Interface for a result post processor for individual result entries.
 */
public interface IPostProcessor {
	/**
	 * Processes a result object, possibly modifying it in the process.
	 * 
	 * @param result
	 *            result object to process
	 * @return modified result object
	 * @throws Exception
	 *             if processing fails
	 */
	public JSONObject process(JSONObject result) throws Exception;
}
