package org.elasticsearch.relay.filters;

import java.util.List;

import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.permissions.UserPermSet;

/**
 * Generic interface for a query filter.
 */
public interface IFilter {
	/**
	 * Filters a query to a single Elasticsearch instance for a specific user
	 * and returns a query object with fewer or additional statements. The given
	 * indices and types may contain "*" or "_all" or be empty, meaning that all
	 * indices or types are to be queried.
	 * 
	 * @param perms
	 *            permissions and other info for the current user
	 * @param query
	 *            query to an Elasticsearch instance
	 * @param indices
	 *            indices that are being queried
	 * @param types
	 *            types that are being queried
	 * @return filtered/modified query
	 */
	public ESQuery addFilter(UserPermSet perms, ESQuery query, List<String> indices, List<String> types);
}
