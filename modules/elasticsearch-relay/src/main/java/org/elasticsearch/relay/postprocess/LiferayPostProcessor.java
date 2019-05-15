package org.elasticsearch.relay.postprocess;

import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.util.ESConstants;
import org.json.JSONObject;

/**
 * Liferay result post processor translating Liferay user IDs to the user IDs
 * used by every other system.
 */
public class LiferayPostProcessor implements IPostProcessor {
	private static final String USER_ID = "userId";
	private static final String LDAP_USER_ID = "ldapUserId";

	private final PermissionCrawler fPerms;

	/**
	 * Creates a Liferay result post processor using the given permission
	 * crawler to look up user IDs using Liferay IDs.
	 * 
	 * @param perms
	 *            permission crawler to use
	 */
	public LiferayPostProcessor(PermissionCrawler perms) {
		fPerms = perms;
	}

	@Override
	public JSONObject process(JSONObject result) throws Exception {
		JSONObject source = result.getJSONObject(ESConstants.R_HIT_SOURCE);

		String liferayId = source.optString(USER_ID);

		if (liferayId != null) {
			String user = fPerms.getUserByLiferayId(liferayId);
			if (user != null) {
				source.put(LDAP_USER_ID, user);
			}
		}

		return result;
	}
}
