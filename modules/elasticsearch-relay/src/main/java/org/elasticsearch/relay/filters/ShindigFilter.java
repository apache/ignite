package org.elasticsearch.relay.filters;

import java.util.List;

import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.util.ESConstants;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Filter for activties, messages and people in the Shindig index. Filters by
 * using origin and whitelist attributes and lets everyone find user profiles.
 */
public class ShindigFilter implements IFilter {
	private static final String SHIN_ORIGIN = "origin";
	private static final String SHIN_WHITELIST = "whitelist";

	private final String fActType;
	private final String fMsgType;
	private final String fPersonType;

	/**
	 * @param actType
	 *            result type for Shindig activities
	 * @param msgType
	 *            result type for Shindig messages
	 * @param personType
	 *            result type for Shindig profiles
	 */
	public ShindigFilter(String actType, String msgType, String personType) {
		fActType = actType;
		fMsgType = msgType;
		fPersonType = personType;
	}

	@Override
	public ESQuery addFilter(UserPermSet perms, ESQuery query, List<String> indices, List<String> types) {
		boolean originFilter = false;

		String user = perms.getUserName();

		if (types.isEmpty() || types.contains(fActType) || types.contains(ESConstants.ALL_FRAGMENT)) {
			try {
				originFilter = true;

				query = actVisibility(user, query);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (types.isEmpty() || types.contains(fMsgType) || types.contains(ESConstants.ALL_FRAGMENT)) {
			originFilter = true;
		}

		if (originFilter) {
			try {
				query = originVisibility(user, query);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// person passthrough - everybody can find them
		if (types.isEmpty() || types.contains(fPersonType) || types.contains(ESConstants.ALL_FRAGMENT)) {
			try {
				personPassthrough(query);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return query;
	}

	private ESQuery actVisibility(String user, ESQuery query) throws Exception {
		// TODO: only visible to owners and their friends
		// TODO: shindig polling will probably be needed

		return query;
	}

	private ESQuery originVisibility(String user, ESQuery query) throws Exception {
		JSONArray filters = query.getAuthFilterOrArr();

		// only sender and recipients can see messages
		JSONObject originFilter = new JSONObject();

		JSONObject termObj = new JSONObject();
		termObj.put(SHIN_ORIGIN, user);

		originFilter.put(ESConstants.Q_TERM, termObj);

		filters.put(originFilter);

		// filter by friends visibility whitelist attribute
		JSONObject whitelistFilter = new JSONObject();

		termObj = new JSONObject();
		termObj.put(SHIN_WHITELIST, user);

		whitelistFilter.put(ESConstants.Q_TERM, termObj);

		filters.put(whitelistFilter);

		return query;
	}

	private ESQuery personPassthrough(ESQuery query) throws Exception {
		JSONArray filters = query.getAuthFilterOrArr();

		JSONObject personFilter = new JSONObject();

		JSONObject termObj = new JSONObject();
		termObj.put(ESConstants.Q_VALUE, fPersonType);

		personFilter.put(ESConstants.Q_TYPE, termObj);

		filters.put(personFilter);

		return query;
	}
}
