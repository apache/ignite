package org.elasticsearch.relay.postprocess;

import java.util.Set;

import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.util.ESConstants;
import com.alibaba.fastjson.JSONObject;

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
	public JSONObject process(JSONObject result) throws Exception {
		JSONObject source = result.getJSONObject(ESConstants.R_HIT_SOURCE);

		String liferayId = source.getString(USER_ID);

		if (liferayId != null) {
			String user = PermissionCrawler.getInstance().getUserByLiferayId(liferayId);
			if (user != null) {
				source.put(LDAP_USER_ID, user);
			}
		}

		return result;
	}
}
