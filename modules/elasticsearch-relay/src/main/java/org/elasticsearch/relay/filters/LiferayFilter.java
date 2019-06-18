package org.elasticsearch.relay.filters;

import java.util.List;
import java.util.Set;

import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.util.ESConstants;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Liferay filter filtering its unspecific entries by their owners' Liferay user
 * ID and roles. Filtering for groups is not yet implemented.
 */
public class LiferayFilter implements IFilter {
	private static final String LR_USER_ID = "userId";
	private static final String LR_ROLE_ID = "roleId";

	private final Set<String> fTypes;
	private final Set<String> fPassRoles;

	/**
	 * @param types
	 *            liferay result types
	 * @param passRoles
	 *            generic roles every user is a member of
	 */
	public LiferayFilter(Set<String> types, Set<String> passRoles) {
		fTypes = types;
		fPassRoles = passRoles;
	}

	@Override
	public ESQuery addFilter(UserPermSet perms, ESQuery query, List<String> indices, List<String> types) {
		JSONArray filters = query.getAuthFilterOrArr();

		String userId = perms.getLiferayId();
		List<String> roleIds = perms.getLiferayRoles();

		// TODO: groups

		try {
			// enable creating user to find entry
			JSONObject userFilter = new JSONObject();
			JSONObject termObject = new JSONObject();

			termObject.put(LR_USER_ID, userId);

			userFilter.put(ESConstants.Q_TERM, termObject);

			filters.add(userFilter);

			// enable users with specific roles to find document
			for (String roleId : roleIds) {
				JSONObject roleFilter = new JSONObject();
				termObject = new JSONObject();

				termObject.put(LR_ROLE_ID, roleId);

				roleFilter.put(ESConstants.Q_TERM, termObject);

				filters.add(roleFilter);
			}

			// enable users with generic roles to find document
			for (String roleId : fPassRoles) {
				JSONObject roleFilter = new JSONObject();
				termObject = new JSONObject();

				termObject.put(LR_ROLE_ID, roleId);

				roleFilter.put(ESConstants.Q_TERM, termObject);

				filters.add(roleFilter);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// TODO: every entry type might need to be treated differently

		// TODO: possibly filter all internal types that are not supposed to be
		// found

		return query;
	}

}
