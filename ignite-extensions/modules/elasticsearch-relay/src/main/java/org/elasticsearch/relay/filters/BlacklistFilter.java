package org.elasticsearch.relay.filters;

import java.util.List;
import java.util.Set;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESDelete;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.ESUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Filter excluding indices and types from the query result. Adjusts query body
 * and query path.
 */
public class BlacklistFilter {
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


	public ESQuery addFilter(ESQuery query) {
		boolean removedLast = false;

		if (fIndices.size()>0) {
			
			if (fIndices.contains(query.getIndices())) {
				removedLast = true;
				query.cancel();				
			} 
		}

		if (!removedLast && fTypes.size()>0) {
			// add blacklisted types

			if (fTypes.contains(query.getTypeName())) {
				// repackage into request path
				//-filterTypes(query,query.getTypeName());
				query.cancel();
			}
		}
		
		return query;
	}
	
	
	
	public ESDelete addFilter(ESDelete query) {
		boolean removedLast = false;

		if (fIndices.size()>0) {
			
			if (fIndices.contains(query.getIndices())) {
				removedLast = true;
				query.cancel();				
			} 
		}

		if (!removedLast && fTypes.size()>0) {
			// add blacklisted types

			if (fTypes.contains(query.getTypeName())) {
				// repackage into request path
				query.cancel();
			}
		}
		
		return query;
	}

	public ESUpdate addFilter(ESUpdate query) {
		boolean removedLast = false;

		if (fIndices.size()>0) {
			
			if (fIndices.contains(query.getIndices())) {
				removedLast = true;
				query.cancel();				
			} 
		}

		if (!removedLast && fTypes.size()>0) {
			// add blacklisted types

			if (fTypes.contains(query.getTypeName())) {
				// repackage into request path
				query.cancel();
			}
		}
		
		return query;
	}
	
	private void filterTypes(ESQuery query,String type) {
		// block problematic types through exclusion
		ArrayNode filters = ESUtil.getOrCreateFilterArray(query);

		ObjectNode notObject = new ObjectNode(ESRelay.jsonNodeFactory);
		ObjectNode typeFilter = new ObjectNode(ESRelay.jsonNodeFactory);
		ObjectNode valueObject = new ObjectNode(ESRelay.jsonNodeFactory);

		valueObject.put(TYPE_VALUE, type);

		typeFilter.set(ESConstants.Q_TYPE, valueObject);

		notObject.set(ESConstants.Q_NOT, typeFilter);
		filters.add(notObject);
	}
}
