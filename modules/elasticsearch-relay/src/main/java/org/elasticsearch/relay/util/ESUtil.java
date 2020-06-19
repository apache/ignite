package org.elasticsearch.relay.util;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESQuery;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class ESUtil {
	private static ObjectNode getFilterObject(ESQuery query) throws Exception {
		// check if there is a query
		ObjectNode jsonQuery = query.getQuery();
		if (jsonQuery == null) {
			jsonQuery = new ObjectNode(ESRelay.jsonNodeFactory);
			query.setQuery(jsonQuery);
		}

		// check if there is a query sub-object
		ObjectNode queryObj = (ObjectNode)jsonQuery.get(ESConstants.Q_QUERY);
		if (queryObj == null) {
			queryObj = new ObjectNode(ESRelay.jsonNodeFactory);
			jsonQuery.put(ESConstants.Q_QUERY, queryObj);
		}

		// check if there is a filtered sub-object
		ObjectNode filteredObj = (ObjectNode)queryObj.get(ESConstants.Q_FILTERED);
		if (filteredObj == null) {
			filteredObj = new ObjectNode(ESRelay.jsonNodeFactory);
			queryObj.put(ESConstants.Q_FILTERED, filteredObj);
		}

		// check if there is a filter sub-object
		ObjectNode filterObj = (ObjectNode)filteredObj.get(ESConstants.Q_FILTER);
		if (filterObj == null) {
			filterObj = new ObjectNode(ESRelay.jsonNodeFactory);
			filteredObj.put(ESConstants.Q_FILTER, filterObj);
		}

		return filterObj;
	}

	public static ArrayNode getOrCreateFilterArray(ESQuery query) throws Exception {
		ObjectNode filterObj = getFilterObject(query);

		// actual array of filters
		// check if there is a logical 'and' array
		ArrayNode andArray = filterObj.withArray(ESConstants.Q_AND);
		if (andArray == null) {
			andArray = new ArrayNode(ESRelay.jsonNodeFactory);
			filterObj.set(ESConstants.Q_AND, andArray);
		}

		return andArray;
	}

	public static void replaceFilterArray(ESQuery query, ArrayNode andArray) throws Exception {
		ObjectNode filterObj = getFilterObject(query);

		// remove existing array
		filterObj.remove(ESConstants.Q_AND);

		// replace with given or new one
		if (andArray == null) {
			andArray = new ArrayNode(ESRelay.jsonNodeFactory);
		}
		filterObj.put(ESConstants.Q_AND, andArray);
	}
}
