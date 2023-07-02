package org.elasticsearch.relay.permissions;

import java.net.URL;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.util.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Crawler retrieving Nuxeo groups for users using Nuxeo's REST API.
 */
public class NuxeoCrawler implements IPermCrawler {
	private final String USER_FRAG = "api/v1/user/";

	private final String PROPERTIES = "properties";
	private final String GROUPS = "groups";
	private final String EXTENDED_GROUPS = "extendedGroups";

	private final String NAME = "name";

	private final String EVERYONE_GROUP = "Everyone";

	private final String fNxUrl;
	private final String fUser;
	private final String fPassword;

	private final Logger fLogger = Logger.getLogger(this.getClass().getName());

	/**
	 * Creates a Nuxeo crawler using the given Nuxeo URL and admin credentials.
	 * 
	 * @param url
	 *            Nuxeo URL
	 * @param user
	 *            Nuxeo admin user
	 * @param pass
	 *            Nuxeo admin password
	 */
	public NuxeoCrawler(String url, String user, String pass) {
		fNxUrl = url + USER_FRAG;
		fUser = user;
		fPassword = pass;
	}

	@Override
	public UserPermSet getPermissions(String name) {
		UserPermSet perms = new UserPermSet(name);

		return getPermissions(perms);
	}

	@Override
	public UserPermSet getPermissions(UserPermSet perms) {
		String user = perms.getUserName();

		try {
			String response = HttpUtil.getAuthenticatedText(new URL(fNxUrl + user), fUser, fPassword);

			ObjectNode userData = (ObjectNode)ESRelay.objectMapper.readTree(response);

			List<String> groups = perms.getNuxeoGroups();

			// remove old groups
			groups.clear();

			ArrayNode userGroups = userData.with(PROPERTIES).withArray(GROUPS);

			for (int i = 0; i < userGroups.size(); ++i) {
				try {
					// sub-object until a certain hotfix
					JsonNode group = userGroups.get(i);
					groups.add(group.get(NAME).asText());
				} catch (Exception e) {
					// simple String later
					groups.add(userGroups.get(i).asText());
				}
			}

			ArrayNode userExGroups = userData.withArray(EXTENDED_GROUPS);
			for (int i = 0; i < userExGroups.size(); ++i) {
				try {
					JsonNode group = userExGroups.get(i);
					groups.add(group.with(NAME).asText());
				} catch (Exception e) {
					// failsafe if groups are listed directly
					groups.add(userExGroups.get(i).asText());
				}
			}

			// add special group "Everyone"
			// TODO: is this reasonable?
			groups.add(EVERYONE_GROUP);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return perms;
	}
}
