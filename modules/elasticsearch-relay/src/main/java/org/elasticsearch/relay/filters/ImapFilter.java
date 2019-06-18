package org.elasticsearch.relay.filters;

import java.util.List;
import java.util.Set;

import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.util.ESConstants;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Filters emails by the ID of the user owning the specific mailbox.
 */
public class ImapFilter implements IFilter {
	private static final String ORIGIN_USER_ID = "userId";

	private final Set<String> fTypes;

	/**
	 * @param types
	 *            mail result types
	 */
	public ImapFilter(Set<String> types) {
		fTypes = types;
	}

	@Override
	public ESQuery addFilter(UserPermSet perms, ESQuery query, List<String> indices, List<String> types) {
		JSONArray filters = query.getAuthFilterOrArr();

		String user = perms.getUserName();

		// TODO: set special field before indexing to simplify?

		try {
			JSONObject originFilter = new JSONObject();

			// only sender and recipients can see messages
			// TODO: filter by senders, recipients or by the folder the mail
			// originates from?

			// Regexp ?
			// http://stackoverflow.com/questions/30473653/elastic-query-dsl-wildcards-in-terms-filter

			JSONObject originMatch = new JSONObject();
			originMatch.put(ORIGIN_USER_ID, user);

			originFilter.put(ESConstants.Q_TERM, originMatch);

			filters.add(originFilter);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return query;
	}

}
