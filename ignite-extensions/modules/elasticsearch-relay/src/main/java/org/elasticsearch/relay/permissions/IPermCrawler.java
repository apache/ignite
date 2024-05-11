package org.elasticsearch.relay.permissions;

/**
 * Interface for a permission crawler retrieving or enriching permission sets
 * for users.
 */
public interface IPermCrawler {
	/**
	 * Retrieves a new permission set for a user ID.
	 * 
	 * @param name
	 *            ID of the user
	 * @return new permission set object
	 */
	public UserPermSet getPermissions(String name);

	/**
	 * Enriches a user's permission set.
	 * 
	 * @param perms
	 *            permission set to enrich
	 * @return enriched permission set
	 */
	public UserPermSet getPermissions(UserPermSet perms);
}
