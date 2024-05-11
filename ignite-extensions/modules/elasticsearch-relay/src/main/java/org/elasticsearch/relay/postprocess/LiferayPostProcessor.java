package org.elasticsearch.relay.postprocess;

import java.util.Set;

import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.util.ESConstants;

import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Liferay result post processor translating Liferay user IDs to the user IDs
 * used by every other system.
 */
public class LiferayPostProcessor implements IPostProcessor {
	private final String USER_ID = "userId";
	private final String LDAP_USER_ID = "ldapUserId";

	
	
	private Set<String>  typeSet= null;
	
	public Set<String> getTypeSet() {
		return typeSet;
	}

	public void setTypeSet(Set<String> typeSet) {
		this.typeSet = typeSet;
	}


	@Override
	public ObjectNode process(ObjectNode result) throws Exception {
		ObjectNode source = result.with(ESConstants.R_HIT_SOURCE);

		String liferayId = source.get(USER_ID).asText();

		if (liferayId != null) {
			String user = PermissionCrawler.getInstance().getUserByLiferayId(liferayId);
			if (user != null) {
				source.put(LDAP_USER_ID, user);
			}
		}

		return result;
	}
}
