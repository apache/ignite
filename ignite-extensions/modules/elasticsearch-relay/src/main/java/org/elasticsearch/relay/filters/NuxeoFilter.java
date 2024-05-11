package org.elasticsearch.relay.filters;

import java.util.List;
import java.util.Set;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.permissions.UserPermSet;
import org.elasticsearch.relay.util.ESConstants;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Nuxeo filter allowing objects' creators and people and groups who were given
 * permission to find entries.
 */
public class NuxeoFilter implements IFilter {
	private static final String NX_CREATOR = "dc:creator";
	private static final String NX_ACL = "ecm:acl";

	private final Set<String> fTypes;

	/**
	 * @param types
	 *            Nuxeo result types
	 */
	public NuxeoFilter(Set<String> types) {
		fTypes = types;
	}

	@Override
	public ESQuery addFilter(UserPermSet perms, ESQuery query, List<String> indices, List<String> types) {
		String user = perms.getUserName();

		try {
			ArrayNode filters = query.getAuthFilterOrArr();

			// enable creator to find documents
			ObjectNode creatorFilter = new ObjectNode(ESRelay.jsonNodeFactory);

			ObjectNode termObj = new ObjectNode(ESRelay.jsonNodeFactory);
			termObj.put(NX_CREATOR, user);

			// TODO: what if permissions are taken away from a user

			creatorFilter.put(ESConstants.Q_TERM, termObj);

			filters.add(creatorFilter);

			// filter by additional ACL rules
			ObjectNode aclFilter = new ObjectNode(ESRelay.jsonNodeFactory);

			termObj = new ObjectNode(ESRelay.jsonNodeFactory);
			termObj.put(NX_ACL, user);

			aclFilter.put(ESConstants.Q_TERM, termObj);

			filters.add(aclFilter);

			// TODO: evaluate roles via ecm:acl
			for (String group : perms.getNuxeoGroups()) {
				aclFilter = new ObjectNode(ESRelay.jsonNodeFactory);

				termObj = new ObjectNode(ESRelay.jsonNodeFactory);
				termObj.put(NX_ACL, group);

				aclFilter.put(ESConstants.Q_TERM, termObj);

				filters.add(aclFilter);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return query;
	}

}
