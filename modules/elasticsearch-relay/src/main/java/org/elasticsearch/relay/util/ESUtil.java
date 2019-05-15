package org.elasticsearch.relay.util;

import org.elasticsearch.relay.model.ESQuery;
import org.json.JSONArray;
import org.json.JSONObject;

public class ESUtil {
	private static JSONObject getFilterObject(ESQuery query) throws Exception {
		// check if there is a query
		JSONObject jsonQuery = query.getQuery();
		if (jsonQuery == null) {
			jsonQuery = new JSONObject();
			query.setQuery(jsonQuery);
		}

		// check if there is a query sub-object
		JSONObject queryObj = jsonQuery.optJSONObject(ESConstants.Q_QUERY);
		if (queryObj == null) {
			queryObj = new JSONObject();
			jsonQuery.put(ESConstants.Q_QUERY, queryObj);
		}

		// check if there is a filtered sub-object
		JSONObject filteredObj = queryObj.optJSONObject(ESConstants.Q_FILTERED);
		if (filteredObj == null) {
			filteredObj = new JSONObject();
			queryObj.put(ESConstants.Q_FILTERED, filteredObj);
		}

		// check if there is a filter sub-object
		JSONObject filterObj = filteredObj.optJSONObject(ESConstants.Q_FILTER);
		if (filterObj == null) {
			filterObj = new JSONObject();
			filteredObj.put(ESConstants.Q_FILTER, filterObj);
		}

		return filterObj;
	}

	public static JSONArray getOrCreateFilterArray(ESQuery query) throws Exception {
		JSONObject filterObj = getFilterObject(query);

		// actual array of filters
		// check if there is a logical 'and' array
		JSONArray andArray = filterObj.optJSONArray(ESConstants.Q_AND);
		if (andArray == null) {
			andArray = new JSONArray();
			filterObj.put(ESConstants.Q_AND, andArray);
		}

		return andArray;
	}

	public static void replaceFilterArray(ESQuery query, JSONArray andArray) throws Exception {
		JSONObject filterObj = getFilterObject(query);

		// remove existing array
		filterObj.remove(ESConstants.Q_AND);

		// replace with given or new one
		if (andArray == null) {
			andArray = new JSONArray();
		}
		filterObj.put(ESConstants.Q_AND, andArray);
	}
}
