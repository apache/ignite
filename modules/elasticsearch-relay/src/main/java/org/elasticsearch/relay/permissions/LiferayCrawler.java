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
 * Liferay crawler retrieving users' Liferay IDs and Liferay Role IDs using
 * Liferay's JSON WS API.
 */
public class LiferayCrawler implements IPermCrawler {
	private static final String USER_BY_SCR_NAME_FRAG = "api/jsonws/user/get-user-id-by-screen-name/company-id/";
	private static final String SCREEN_NAME_FRAG = "/screen-name/";

	private static final String USER_ROLES_FRAG = "api/jsonws/role/get-user-roles/user-id/";

	private static final String LR_ROLE_ID = "roleId";

	private final String fLrUrl;
	private final String fCompany;
	private final String fUser;
	private final String fPassword;

	private final Logger fLogger;

	/**
	 * Creates a new liferay crawler using the given Liferay URL, company ID and
	 * retrieval credentials.
	 * 
	 * @param url
	 *            base URL of liferay
	 * @param company
	 *            ID of the company the users are in
	 * @param user
	 *            user to authenticate with
	 * @param pass
	 *            password to authenticate with
	 */
	public LiferayCrawler(String url, String company, String user, String pass) {
		fLrUrl = url;
		fCompany = company;
		fUser = user;
		fPassword = pass;

		fLogger = Logger.getLogger(this.getClass().getName());
	}

	@Override
	public UserPermSet getPermissions(String name) {
		UserPermSet perms = new UserPermSet(name);

		return getPermissions(perms);
	}

	@Override
	public UserPermSet getPermissions(UserPermSet perms) {
		String screenName = perms.getUserName();

		try {
			// get Liferay user ID
			String response = HttpUtil.getAuthenticatedText(
					new URL(fLrUrl + USER_BY_SCR_NAME_FRAG + fCompany + SCREEN_NAME_FRAG + screenName), fUser,
					fPassword);

			String userId = response.replaceAll("\"", "");
			perms.setLiferayId(userId);

			// TODO: get Liferay groups

			// clear old roles
			List<String> roles = perms.getLiferayRoles();
			roles.clear();

			// get Liferay Roles
			response = HttpUtil.getAuthenticatedText(new URL(fLrUrl + USER_ROLES_FRAG + userId), fUser, fPassword);
			JsonNode node = ESRelay.objectMapper.readTree(response);
			ArrayNode rolesArray = (ArrayNode)node;
			for (int i = 0; i < rolesArray.size(); ++i) {
				roles.add(rolesArray.get(i).get(LR_ROLE_ID).asText());
			}
		} catch (Exception e) {
			fLogger.log(Level.SEVERE, "failed to retrieve Liferay permission information.", e);
		}

		return perms;
	}

}
