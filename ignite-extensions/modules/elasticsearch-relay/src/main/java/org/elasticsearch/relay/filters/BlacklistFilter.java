package org.elasticsearch.relay.filters;

import java.util.List;
import java.util.Set;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.ESUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Filter excluding indices and types from the query result. Adjusts query body
 * and query path.
 */
public class BlacklistFilter implements IFilter {
	private static final String TYPE_VALUE = "value";

	private final Set<String> fIndices; // black indices
	private final Set<String> fTypes; // black types

	/**
	 * Given blacklist sets must not be null.
	 * 
	 * @param indices
	 *            indices to filter out
	 * @param types
	 *            result types to filter out
	 */
	public BlacklistFilter(Set<String> indices, Set<String> types) {
		fIndices = indices;
		fTypes = types;
	}

	@Override
	public ESQuery addFilter(UserPermSet perms, ESQuery query, List<String> indices, List<String> types) {
		boolean allIndices = false;
		boolean allTypes = false;

		// check if all indices are to be searched
		
		if (indices.isEmpty() || indices.size() == 1 && indices.get(0).equals(ESConstants.WILDCARD)) {
			allIndices = true;
		}

		// check if all types are to be searched
		if (types.isEmpty() || types.size() == 1 && types.get(0).equals(ESConstants.WILDCARD)) {
			allTypes = true;
		}

		boolean removedLast = false;

		if (fIndices.size()>0) {
			// remove blacklisted indices
			indices.removeAll(fIndices);

			if (indices.isEmpty()) {
				removedLast = true;
			} else {
				// repackage into request path
				replaceInPath(query, indices);
			}
		}

		if (!allTypes && fTypes.size()>0) {
			// remove blacklisted types
			types.removeAll(fTypes);

			if (types.isEmpty()) {
				removedLast = true;
			} else {
				// repackage into request path
				try {
					filterTypes(query);
				} catch (Exception e) {				
					e.printStackTrace();
				}
			}
		}
		
		// check that if all indices or types were removed the query won't pass as "get all"
		if (removedLast) {
			query.cancel();
		}

		return query;
	}

	private void filterTypes(ESQuery query) throws Exception {
		// block problematic types through exclusion
		ArrayNode filters = ESUtil.getOrCreateFilterArray(query);

		for (String type : fTypes) {
			ObjectNode notObject = new ObjectNode(ESRelay.jsonNodeFactory);
			ObjectNode typeFilter = new ObjectNode(ESRelay.jsonNodeFactory);
			ObjectNode valueObject = new ObjectNode(ESRelay.jsonNodeFactory);

			valueObject.put(TYPE_VALUE, type);

			typeFilter.set(ESConstants.Q_TYPE, valueObject);

			notObject.set(ESConstants.Q_NOT, typeFilter);
			filters.add(notObject);
		}
	}

	private void replaceInPath(ESQuery query, List<String> entries) {
		String fragment = "";
		for (String entry : entries) {
			fragment += entry + ",";
		}
		fragment = fragment.substring(0, fragment.length() - 1);
		query.setIndices(fragment);
	}
}
